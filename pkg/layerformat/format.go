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
	magic        = "AFSLYR01"
	fixedHdrSize = len(magic) + 8
	// Increase gzip source read buffer to reduce tiny ReaderAt calls (default is 4KiB).
	gzipReadBufferSize = 1 << 20
)

type EntryType string

const (
	EntryTypeDir     EntryType = "dir"
	EntryTypeFile    EntryType = "file"
	EntryTypeSymlink EntryType = "symlink"
)

// Entry is one object in archive metadata.
type Entry struct {
	Path             string    `json:"path"`
	Type             EntryType `json:"type"`
	Mode             uint32    `json:"mode"`
	UID              int       `json:"uid,omitempty"`
	GID              int       `json:"gid,omitempty"`
	ModTimeUnix      int64     `json:"mod_time_unix,omitempty"`
	SymlinkTarget    string    `json:"symlink_target,omitempty"`
	UncompressedSize int64     `json:"uncompressed_size,omitempty"`
	CompressedOffset int64     `json:"compressed_offset,omitempty"`
	CompressedSize   int64     `json:"compressed_size,omitempty"`
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

	if _, err := out.Write([]byte(magic)); err != nil {
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
	if string(hdr[:len(magic)]) != magic {
		return nil, fmt.Errorf("invalid magic")
	}
	tocLen := binary.LittleEndian.Uint64(hdr[len(magic):])
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
	return &Reader{ra: ra, toc: t, entriesByP: entriesByP, dataStart: int64(fixedHdrSize) + int64(tocLen)}, nil
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
