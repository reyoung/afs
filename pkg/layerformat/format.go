package layerformat

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"
)

const (
	magic01      = "AFSLYR01"
	magic02      = "AFSLYR02"
	magicLen     = 8 // both magics are exactly 8 bytes
	fixedHdrSize = magicLen + 8
	// Increase gzip source read buffer to reduce tiny ReaderAt calls (default is 4KiB).
	gzipReadBufferSize = 1 << 20
)

// FormatVersion identifies the AFS layer format.
type FormatVersion int

const (
	FormatV1 FormatVersion = 1
	FormatV2 FormatVersion = 2
)

// PayloadCodec describes how a regular file's payload is stored in the archive.
type PayloadCodec string

const (
	PayloadCodecGzip     PayloadCodec = "gzip"
	PayloadCodecIdentity PayloadCodec = "identity"
)

type EntryType string

const (
	EntryTypeDir     EntryType = "dir"
	EntryTypeFile    EntryType = "file"
	EntryTypeSymlink EntryType = "symlink"
)

// Entry is one object in archive metadata.
type Entry struct {
	Path             string       `json:"path"`
	Type             EntryType    `json:"type"`
	Mode             uint32       `json:"mode"`
	UID              int          `json:"uid,omitempty"`
	GID              int          `json:"gid,omitempty"`
	ModTimeUnix      int64        `json:"mod_time_unix,omitempty"`
	SymlinkTarget    string       `json:"symlink_target,omitempty"`
	UncompressedSize int64        `json:"uncompressed_size,omitempty"`
	CompressedOffset int64        `json:"compressed_offset,omitempty"`
	CompressedSize   int64        `json:"compressed_size,omitempty"`
	PayloadCodec     PayloadCodec `json:"payload_codec,omitempty"`
	PayloadOffset    int64        `json:"payload_offset,omitempty"`
	PayloadSize      int64        `json:"payload_size,omitempty"`
}

func (e Entry) ModTime() time.Time {
	if e.ModTimeUnix == 0 {
		return time.Unix(0, 0)
	}
	return time.Unix(e.ModTimeUnix, 0)
}

type toc struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

type hardlinkRef struct {
	entryPath  string
	linkName   string
	candidates []string
}

// ConvertTarGzipToArchive converts an OCI layer tar+gzip stream into the custom format.
// Files are individually gzip compressed so readers can locate a single file directly.
func ConvertTarGzipToArchive(layerTarGzip io.Reader, out io.Writer) error {
	gz, err := gzip.NewReader(layerTarGzip)
	if err != nil {
		return fmt.Errorf("open layer gzip: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)

	entries := make([]Entry, 0, 128)
	dataChunks := make([][]byte, 0, 128)
	entryIndexByPath := make(map[string]int, 128)
	pendingHardlinks := make(map[int]hardlinkRef)
	var dataOffset int64

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}

		normalized, ok := normalizeTarPath(hdr.Name)
		if !ok {
			continue
		}

		base := Entry{
			Path:        normalized,
			Mode:        uint32(hdr.FileInfo().Mode().Perm()),
			UID:         hdr.Uid,
			GID:         hdr.Gid,
			ModTimeUnix: hdr.ModTime.Unix(),
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			base.Type = EntryTypeDir
			entries = append(entries, base)
			entryIndexByPath[normalized] = len(entries) - 1
		case tar.TypeSymlink:
			base.Type = EntryTypeSymlink
			base.SymlinkTarget = hdr.Linkname
			entries = append(entries, base)
			entryIndexByPath[normalized] = len(entries) - 1
		case tar.TypeReg, tar.TypeRegA:
			base.Type = EntryTypeFile
			base.UncompressedSize = hdr.Size

			compressed, err := gzipCompressFromReader(tr)
			if err != nil {
				return fmt.Errorf("compress %s: %w", normalized, err)
			}
			base.CompressedOffset = dataOffset
			base.CompressedSize = int64(len(compressed))
			dataOffset += int64(len(compressed))
			entries = append(entries, base)
			dataChunks = append(dataChunks, compressed)
			entryIndexByPath[normalized] = len(entries) - 1
		case tar.TypeLink:
			candidates := hardlinkTargetCandidates(normalized, hdr.Linkname)
			if len(candidates) == 0 {
				return fmt.Errorf("hardlink %s has invalid target %q", normalized, hdr.Linkname)
			}
			base.Type = EntryTypeFile
			entries = append(entries, base)
			idx := len(entries) - 1
			entryIndexByPath[normalized] = idx
			pendingHardlinks[idx] = hardlinkRef{
				entryPath:  normalized,
				linkName:   hdr.Linkname,
				candidates: candidates,
			}
		default:
			// Skip unsupported tar objects (block devices, fifos, etc).
		}
	}

	if err := resolveHardlinks(entries, pendingHardlinks, entryIndexByPath); err != nil {
		return err
	}

	t := toc{Version: 1, Entries: entries}
	tocBytes, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal toc: %w", err)
	}

	if _, err := out.Write([]byte(magic01)); err != nil {
		return fmt.Errorf("write magic: %w", err)
	}
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(tocBytes)))
	if _, err := out.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write toc len: %w", err)
	}
	if _, err := out.Write(tocBytes); err != nil {
		return fmt.Errorf("write toc: %w", err)
	}
	for _, chunk := range dataChunks {
		if _, err := out.Write(chunk); err != nil {
			return fmt.Errorf("write file payload: %w", err)
		}
	}
	return nil
}

// ConvertTarGzipToArchiveV2 converts an OCI layer tar+gzip stream into AFSLYR02 format.
// Regular file payloads are stored as identity (plain, uncompressed).
func ConvertTarGzipToArchiveV2(layerTarGzip io.Reader, out io.Writer) error {
	gz, err := gzip.NewReader(layerTarGzip)
	if err != nil {
		return fmt.Errorf("open layer gzip: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)

	entries := make([]Entry, 0, 128)
	dataChunks := make([][]byte, 0, 128)
	entryIndexByPath := make(map[string]int, 128)
	pendingHardlinks := make(map[int]hardlinkRef)
	var dataOffset int64

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}

		normalized, ok := normalizeTarPath(hdr.Name)
		if !ok {
			continue
		}

		base := Entry{
			Path:        normalized,
			Mode:        uint32(hdr.FileInfo().Mode().Perm()),
			UID:         hdr.Uid,
			GID:         hdr.Gid,
			ModTimeUnix: hdr.ModTime.Unix(),
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			base.Type = EntryTypeDir
			entries = append(entries, base)
			entryIndexByPath[normalized] = len(entries) - 1
		case tar.TypeSymlink:
			base.Type = EntryTypeSymlink
			base.SymlinkTarget = hdr.Linkname
			entries = append(entries, base)
			entryIndexByPath[normalized] = len(entries) - 1
		case tar.TypeReg, tar.TypeRegA:
			base.Type = EntryTypeFile
			base.UncompressedSize = hdr.Size
			base.PayloadCodec = PayloadCodecIdentity
			base.PayloadOffset = dataOffset
			base.PayloadSize = hdr.Size

			payload, err := io.ReadAll(tr)
			if err != nil {
				return fmt.Errorf("read %s: %w", normalized, err)
			}
			dataOffset += int64(len(payload))
			entries = append(entries, base)
			dataChunks = append(dataChunks, payload)
			entryIndexByPath[normalized] = len(entries) - 1
		case tar.TypeLink:
			candidates := hardlinkTargetCandidates(normalized, hdr.Linkname)
			if len(candidates) == 0 {
				return fmt.Errorf("hardlink %s has invalid target %q", normalized, hdr.Linkname)
			}
			base.Type = EntryTypeFile
			entries = append(entries, base)
			idx := len(entries) - 1
			entryIndexByPath[normalized] = idx
			pendingHardlinks[idx] = hardlinkRef{
				entryPath:  normalized,
				linkName:   hdr.Linkname,
				candidates: candidates,
			}
		default:
			// Skip unsupported tar objects.
		}
	}

	if err := resolveHardlinksV2(entries, pendingHardlinks, entryIndexByPath); err != nil {
		return err
	}

	t := toc{Version: 2, Entries: entries}
	tocBytes, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal toc: %w", err)
	}

	if _, err := out.Write([]byte(magic02)); err != nil {
		return fmt.Errorf("write magic: %w", err)
	}
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(tocBytes)))
	if _, err := out.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write toc len: %w", err)
	}
	if _, err := out.Write(tocBytes); err != nil {
		return fmt.Errorf("write toc: %w", err)
	}
	for _, chunk := range dataChunks {
		if _, err := out.Write(chunk); err != nil {
			return fmt.Errorf("write file payload: %w", err)
		}
	}
	return nil
}

// resolveHardlinksV2 resolves hardlinks using V2 payload fields.
func resolveHardlinksV2(entries []Entry, pending map[int]hardlinkRef, entryIndexByPath map[string]int) error {
	resolving := make(map[int]struct{}, len(pending))
	resolved := make(map[int]struct{}, len(pending))

	var resolve func(idx int) error
	resolve = func(idx int) error {
		if _, ok := resolved[idx]; ok {
			return nil
		}
		ref, ok := pending[idx]
		if !ok {
			return nil
		}
		if _, ok := resolving[idx]; ok {
			return fmt.Errorf("hardlink cycle detected for %s", ref.entryPath)
		}
		resolving[idx] = struct{}{}
		defer delete(resolving, idx)

		for _, candidate := range ref.candidates {
			targetIdx, ok := entryIndexByPath[candidate]
			if !ok {
				continue
			}
			if targetIdx == idx {
				return fmt.Errorf("hardlink %s points to itself", ref.entryPath)
			}
			if err := resolve(targetIdx); err != nil {
				return err
			}
			target := entries[targetIdx]
			if target.Type != EntryTypeFile {
				return fmt.Errorf("hardlink %s target %s is not a regular file", ref.entryPath, candidate)
			}
			entries[idx].UncompressedSize = target.UncompressedSize
			entries[idx].PayloadCodec = target.PayloadCodec
			entries[idx].PayloadOffset = target.PayloadOffset
			entries[idx].PayloadSize = target.PayloadSize
			delete(pending, idx)
			resolved[idx] = struct{}{}
			return nil
		}
		return fmt.Errorf("hardlink %s target %q not found in layer", ref.entryPath, ref.linkName)
	}

	for idx := range pending {
		if err := resolve(idx); err != nil {
			return err
		}
	}
	return nil
}

// Reader can open files from the custom archive.
type Reader struct {
	ra            io.ReaderAt
	toc           toc
	entriesByP    map[string]Entry
	dataStart     int64
	formatVersion FormatVersion
}

// NewReader parses archive metadata from readerAt.
func NewReader(ra io.ReaderAt) (*Reader, error) {
	hdr := make([]byte, fixedHdrSize)
	if _, err := ra.ReadAt(hdr, 0); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	magicStr := string(hdr[:magicLen])
	var fv FormatVersion
	switch magicStr {
	case magic01:
		fv = FormatV1
	case magic02:
		fv = FormatV2
	default:
		return nil, fmt.Errorf("invalid magic: %q", magicStr)
	}
	tocLen := binary.LittleEndian.Uint64(hdr[magicLen:])
	tocBytes := make([]byte, tocLen)
	if _, err := ra.ReadAt(tocBytes, int64(fixedHdrSize)); err != nil {
		return nil, fmt.Errorf("read toc: %w", err)
	}
	var t toc
	if err := json.Unmarshal(tocBytes, &t); err != nil {
		return nil, fmt.Errorf("decode toc: %w", err)
	}
	entriesByP := make(map[string]Entry, len(t.Entries))
	for _, e := range t.Entries {
		entriesByP[e.Path] = e
	}
	return &Reader{ra: ra, toc: t, entriesByP: entriesByP, dataStart: int64(fixedHdrSize) + int64(tocLen), formatVersion: fv}, nil
}

// FormatVersion returns the format version of the archive.
func (r *Reader) FormatVersion() FormatVersion {
	return r.formatVersion
}

func (r *Reader) Entries() []Entry {
	out := make([]Entry, len(r.toc.Entries))
	copy(out, r.toc.Entries)
	return out
}

func (r *Reader) Stat(p string) (Entry, error) {
	norm, ok := normalizeTarPath(p)
	if !ok {
		return Entry{}, fs.ErrNotExist
	}
	e, ok := r.entriesByP[norm]
	if !ok {
		return Entry{}, fs.ErrNotExist
	}
	return e, nil
}

// ReadFile returns uncompressed bytes of one file.
func (r *Reader) ReadFile(p string) ([]byte, error) {
	e, err := r.Stat(p)
	if err != nil {
		return nil, err
	}
	if e.Type != EntryTypeFile {
		return nil, fmt.Errorf("%s is not a regular file", p)
	}

	if r.formatVersion == FormatV2 {
		buf := make([]byte, e.PayloadSize)
		if _, err := r.ra.ReadAt(buf, r.dataStart+e.PayloadOffset); err != nil {
			return nil, fmt.Errorf("read file data %s: %w", p, err)
		}
		return buf, nil
	}

	// V1: gzip compressed
	section := io.NewSectionReader(r.ra, r.dataStart+e.CompressedOffset, e.CompressedSize)
	gz, err := gzip.NewReader(bufio.NewReaderSize(section, gzipReadBufferSize))
	if err != nil {
		return nil, fmt.Errorf("open file gzip %s: %w", p, err)
	}
	defer gz.Close()

	b, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("read file data %s: %w", p, err)
	}
	return b, nil
}

// CopyFile streams one file's uncompressed bytes into dst.
func (r *Reader) CopyFile(p string, dst io.Writer) (int64, error) {
	e, err := r.Stat(p)
	if err != nil {
		return 0, err
	}
	if e.Type != EntryTypeFile {
		return 0, fmt.Errorf("%s is not a regular file", p)
	}

	if r.formatVersion == FormatV2 {
		section := io.NewSectionReader(r.ra, r.dataStart+e.PayloadOffset, e.PayloadSize)
		n, err := io.Copy(dst, section)
		if err != nil {
			return n, fmt.Errorf("copy file data %s: %w", p, err)
		}
		return n, nil
	}

	// V1: gzip compressed
	section := io.NewSectionReader(r.ra, r.dataStart+e.CompressedOffset, e.CompressedSize)
	gz, err := gzip.NewReader(bufio.NewReaderSize(section, gzipReadBufferSize))
	if err != nil {
		return 0, fmt.Errorf("open file gzip %s: %w", p, err)
	}
	defer gz.Close()

	n, err := io.Copy(dst, gz)
	if err != nil {
		return n, fmt.Errorf("copy file data %s: %w", p, err)
	}
	return n, nil
}

// FileSection holds the information needed to perform random reads on an AFSLYR02 plain payload.
type FileSection struct {
	RA     io.ReaderAt
	Offset int64
	Size   int64
}

// ReadAt implements io.ReaderAt for the file section.
func (fs FileSection) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= fs.Size {
		return 0, io.EOF
	}
	remaining := fs.Size - off
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := fs.RA.ReadAt(p, fs.Offset+off)
	if err == io.EOF && int64(n) == remaining {
		// Underlying reader may return EOF when reading to end of section,
		// but only signal EOF when we've consumed the entire file.
	}
	return n, err
}

// OpenFileSection returns a FileSection for direct random-access reading of
// an AFSLYR02 plain payload. Returns an error for AFSLYR01 archives.
func (r *Reader) OpenFileSection(p string) (FileSection, error) {
	if r.formatVersion != FormatV2 {
		return FileSection{}, fmt.Errorf("OpenFileSection requires AFSLYR02 format")
	}
	e, err := r.Stat(p)
	if err != nil {
		return FileSection{}, err
	}
	if e.Type != EntryTypeFile {
		return FileSection{}, fmt.Errorf("%s is not a regular file", p)
	}
	return FileSection{
		RA:     r.ra,
		Offset: r.dataStart + e.PayloadOffset,
		Size:   e.PayloadSize,
	}, nil
}

// ReadFileAt reads up to len(dst) bytes from the file at path starting at offset off.
// For AFSLYR02, this reads directly from the archive without decompression.
// For AFSLYR01, this falls back to reading the entire file and slicing.
func (r *Reader) ReadFileAt(p string, dst []byte, off int64) (int, error) {
	e, err := r.Stat(p)
	if err != nil {
		return 0, err
	}
	if e.Type != EntryTypeFile {
		return 0, fmt.Errorf("%s is not a regular file", p)
	}

	if r.formatVersion == FormatV2 {
		if off >= e.PayloadSize {
			return 0, io.EOF
		}
		remaining := e.PayloadSize - off
		if int64(len(dst)) > remaining {
			dst = dst[:remaining]
		}
		return r.ra.ReadAt(dst, r.dataStart+e.PayloadOffset+off)
	}

	// V1 fallback: decompress entire file
	data, err := r.ReadFile(p)
	if err != nil {
		return 0, err
	}
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(dst, data[off:])
	return n, nil
}

func gzipCompressFromReader(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := io.Copy(gz, r); err != nil {
		_ = gz.Close()
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func hardlinkTargetCandidates(entryPath string, linkName string) []string {
	raw := strings.TrimSpace(linkName)
	if raw == "" {
		return nil
	}
	out := make([]string, 0, 2)
	seen := make(map[string]struct{}, 2)
	appendIfValid := func(p string) {
		norm, ok := normalizeTarPath(p)
		if !ok {
			return
		}
		if _, exists := seen[norm]; exists {
			return
		}
		seen[norm] = struct{}{}
		out = append(out, norm)
	}
	appendIfValid(raw)
	parent := path.Dir(entryPath)
	if parent == "." {
		parent = ""
	}
	appendIfValid(path.Join(parent, raw))
	return out
}

func resolveHardlinks(entries []Entry, pending map[int]hardlinkRef, entryIndexByPath map[string]int) error {
	resolving := make(map[int]struct{}, len(pending))
	resolved := make(map[int]struct{}, len(pending))

	var resolve func(idx int) error
	resolve = func(idx int) error {
		if _, ok := resolved[idx]; ok {
			return nil
		}
		ref, ok := pending[idx]
		if !ok {
			return nil
		}
		if _, ok := resolving[idx]; ok {
			return fmt.Errorf("hardlink cycle detected for %s", ref.entryPath)
		}
		resolving[idx] = struct{}{}
		defer delete(resolving, idx)

		for _, candidate := range ref.candidates {
			targetIdx, ok := entryIndexByPath[candidate]
			if !ok {
				continue
			}
			if targetIdx == idx {
				return fmt.Errorf("hardlink %s points to itself", ref.entryPath)
			}
			if err := resolve(targetIdx); err != nil {
				return err
			}
			target := entries[targetIdx]
			if target.Type != EntryTypeFile {
				return fmt.Errorf("hardlink %s target %s is not a regular file", ref.entryPath, candidate)
			}
			entries[idx].UncompressedSize = target.UncompressedSize
			entries[idx].CompressedOffset = target.CompressedOffset
			entries[idx].CompressedSize = target.CompressedSize
			delete(pending, idx)
			resolved[idx] = struct{}{}
			return nil
		}
		return fmt.Errorf("hardlink %s target %q not found in layer", ref.entryPath, ref.linkName)
	}

	for idx := range pending {
		if err := resolve(idx); err != nil {
			return err
		}
	}
	return nil
}

func normalizeTarPath(p string) (string, bool) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", false
	}
	p = strings.TrimPrefix(p, "./")
	clean := path.Clean("/" + p)
	if clean == "/" {
		return "", false
	}
	clean = strings.TrimPrefix(clean, "/")
	if strings.HasPrefix(clean, "../") || clean == ".." {
		return "", false
	}
	return clean, true
}
