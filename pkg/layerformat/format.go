package layerformat

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"
)

const (
	magic02      = "AFSLYR02"
	magicLen     = 8
	fixedHdrSize = magicLen + 8

	// MinValidSize is the minimum byte size for a valid AFSLYR02 file:
	// 8 bytes magic + 8 bytes TOC length.
	MinValidSize = fixedHdrSize
)

// PayloadCodec describes how a regular file's payload is stored in the archive.
type PayloadCodec string

const (
	PayloadCodecIdentity PayloadCodec = "identity"
)

type EntryType string

const (
	EntryTypeDir     EntryType = "dir"
	EntryTypeFile    EntryType = "file"
	EntryTypeSymlink EntryType = "symlink"
)

type ContentKind uint8

const (
	ContentKindUnknown ContentKind = 0
	ContentKindELF     ContentKind = 1
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
	Digest           string       `json:"digest"`
	PayloadCodec     PayloadCodec `json:"payload_codec,omitempty"`
	PayloadOffset    int64        `json:"payload_offset,omitempty"`
	PayloadSize      int64        `json:"payload_size,omitempty"`
	ContentKind      ContentKind  `json:"k,omitempty"`
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

// ConvertTarGzipToArchive converts an OCI layer tar+gzip stream into AFSLYR02 format.
// Regular file payloads are stored as identity (plain, uncompressed).
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
			base.PayloadCodec = PayloadCodecIdentity
			base.PayloadOffset = dataOffset
			base.PayloadSize = hdr.Size

			payload, err := io.ReadAll(tr)
			if err != nil {
				return fmt.Errorf("read %s: %w", normalized, err)
			}
			sum := sha256.Sum256(payload)
			base.Digest = "sha256:" + hex.EncodeToString(sum[:])
			base.ContentKind = detectContentKind(normalized, payload)
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

	if err := resolveHardlinks(entries, pendingHardlinks, entryIndexByPath); err != nil {
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

// resolveHardlinks resolves hardlinks using payload fields.
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
			entries[idx].Digest = target.Digest
			entries[idx].PayloadCodec = target.PayloadCodec
			entries[idx].PayloadOffset = target.PayloadOffset
			entries[idx].PayloadSize = target.PayloadSize
			entries[idx].ContentKind = target.ContentKind
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
	ra         io.ReaderAt
	toc        toc
	entriesByP map[string]Entry
	dataStart  int64
}

// NewReader parses archive metadata from readerAt.
func NewReader(ra io.ReaderAt) (*Reader, error) {
	hdr := make([]byte, fixedHdrSize)
	if _, err := ra.ReadAt(hdr, 0); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	magicStr := string(hdr[:magicLen])
	if magicStr != magic02 {
		return nil, fmt.Errorf("invalid magic: %q (only AFSLYR02 is supported)", magicStr)
	}
	tocLen := binary.LittleEndian.Uint64(hdr[magicLen:])
	var t toc
	tocReader := io.NewSectionReader(ra, int64(fixedHdrSize), int64(tocLen))
	dec := json.NewDecoder(tocReader)
	if err := dec.Decode(&t); err != nil {
		return nil, fmt.Errorf("decode toc: %w", err)
	}
	entriesByP := make(map[string]Entry, len(t.Entries))
	for _, e := range t.Entries {
		entriesByP[e.Path] = e
	}
	return &Reader{ra: ra, toc: t, entriesByP: entriesByP, dataStart: int64(fixedHdrSize) + int64(tocLen)}, nil
}

func detectContentKind(filePath string, payload []byte) ContentKind {
	if len(payload) < 4 {
		return ContentKindUnknown
	}
	if payload[0] != 0x7f || payload[1] != 'E' || payload[2] != 'L' || payload[3] != 'F' {
		return ContentKindUnknown
	}

	f, err := elf.NewFile(bytesReaderAt(payload))
	if err != nil {
		return ContentKindELF
	}
	defer f.Close()

	hasInterp := false
	for _, prog := range f.Progs {
		if prog.Type == elf.PT_INTERP {
			hasInterp = true
			break
		}
	}
	switch f.Type {
	case elf.ET_EXEC:
		return ContentKindELF
	case elf.ET_DYN:
		if hasInterp {
			return ContentKindELF
		}
		return ContentKindELF
	}
	if strings.Contains(path.Base(filePath), ".so") {
		return ContentKindELF
	}
	return ContentKindELF
}

type bytesReaderAt []byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// NewReaderCached is like NewReader but uses a TOC cache keyed by layer digest.
// On cache hit, the JSON TOC parsing and map construction are skipped entirely.
func NewReaderCached(ra io.ReaderAt, cache *TOCCache, digest string) (*Reader, error) {
	if cache != nil {
		if cached, ok := cache.Get(digest); ok {
			return &Reader{
				ra:         ra,
				toc:        cached.tocData,
				entriesByP: cached.entriesByP,
				dataStart:  cached.dataStart,
			}, nil
		}
	}
	r, err := NewReader(ra)
	if err != nil {
		return nil, err
	}
	if cache != nil {
		cache.Put(digest, &cachedTOC{
			tocData:    r.toc,
			entriesByP: r.entriesByP,
			dataStart:  r.dataStart,
		})
	}
	return r, nil
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

	buf := make([]byte, e.PayloadSize)
	if _, err := r.ra.ReadAt(buf, r.dataStart+e.PayloadOffset); err != nil {
		return nil, fmt.Errorf("read file data %s: %w", p, err)
	}
	return buf, nil
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

	section := io.NewSectionReader(r.ra, r.dataStart+e.PayloadOffset, e.PayloadSize)
	n, err := io.Copy(dst, section)
	if err != nil {
		return n, fmt.Errorf("copy file data %s: %w", p, err)
	}
	return n, nil
}

// FileSection holds the information needed to perform random reads on a plain payload.
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

// OpenFileSection returns a FileSection for direct random-access reading.
func (r *Reader) OpenFileSection(p string) (FileSection, error) {
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
func (r *Reader) ReadFileAt(p string, dst []byte, off int64) (int, error) {
	e, err := r.Stat(p)
	if err != nil {
		return 0, err
	}
	if e.Type != EntryTypeFile {
		return 0, fmt.Errorf("%s is not a regular file", p)
	}

	if off >= e.PayloadSize {
		return 0, io.EOF
	}
	remaining := e.PayloadSize - off
	if int64(len(dst)) > remaining {
		dst = dst[:remaining]
	}
	return r.ra.ReadAt(dst, r.dataStart+e.PayloadOffset+off)
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
