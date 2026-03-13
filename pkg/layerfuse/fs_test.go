package layerfuse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
)

type fakeSharedCache struct {
	prepareFn func(digest string, filePath string, fill func(w io.Writer) (int64, error)) (string, int64, error)
}

func (f *fakeSharedCache) Prepare(digest string, filePath string, fill func(w io.Writer) (int64, error)) (string, int64, error) {
	return f.prepareFn(digest, filePath, fill)
}

func TestSetAttrTimes(t *testing.T) {
	t.Parallel()

	out := &fuse.AttrOut{}
	setAttrTimes(out, 1700000000)

	if out.Mtime != 1700000000 {
		t.Fatalf("Mtime=%d, want 1700000000", out.Mtime)
	}
	if out.Atime != 1700000000 {
		t.Fatalf("Atime=%d, want 1700000000", out.Atime)
	}
	if out.Ctime != 1700000000 {
		t.Fatalf("Ctime=%d, want 1700000000", out.Ctime)
	}
}

func TestSetAttrTimesNonPositiveIgnored(t *testing.T) {
	t.Parallel()

	out := &fuse.AttrOut{}
	setAttrTimes(out, 0)
	if out.Mtime != 0 || out.Atime != 0 || out.Ctime != 0 {
		t.Fatalf("timestamps should stay zero, got atime=%d mtime=%d ctime=%d", out.Atime, out.Mtime, out.Ctime)
	}
}

func TestSetEntryOutAttrRegularFile(t *testing.T) {
	t.Parallel()

	out := &fuse.EntryOut{}
	setEntryOutAttr(out, layerformat.Entry{
		Type:             layerformat.EntryTypeFile,
		Mode:             0o755,
		UncompressedSize: 1234,
		ModTimeUnix:      1700000000,
	})

	if out.Mode != uint32(syscall.S_IFREG|0o755) {
		t.Fatalf("Mode=%o, want %o", out.Mode, uint32(syscall.S_IFREG|0o755))
	}
	if out.Size != 1234 {
		t.Fatalf("Size=%d, want 1234", out.Size)
	}
	if out.Mtime != 1700000000 {
		t.Fatalf("Mtime=%d, want 1700000000", out.Mtime)
	}
}

func TestFileNodePrepareTempFileUsesSharedCacheHit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	p := filepath.Join(dir, "hit.spill")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write cache file: %v", err)
	}

	f := &FileNode{
		entry:       layerformat.Entry{Path: "/a/b"},
		layerDigest: "sha256:abc",
		sharedCache: &fakeSharedCache{
			prepareFn: func(digest string, filePath string, fill func(w io.Writer) (int64, error)) (string, int64, error) {
				if digest != "sha256:abc" || filePath != "/a/b" {
					t.Fatalf("unexpected shared cache key digest=%q filePath=%q", digest, filePath)
				}
				return p, 5, nil
			},
		},
	}

	fd, n, err := f.prepareTempFile()
	if err != nil {
		t.Fatalf("prepareTempFile: %v", err)
	}
	defer fd.Close()
	if n != 5 {
		t.Fatalf("size=%d, want 5", n)
	}
}

func TestFileNodePrepareTempFileFallsBackWhenSharedCacheOpenFails(t *testing.T) {
	t.Parallel()

	const (
		filePath = "a.txt"
		content  = "hello-from-fallback"
	)
	reader := newReaderWithSingleFile(t, filePath, []byte(content))
	entry, err := reader.Stat(filePath)
	if err != nil {
		t.Fatalf("reader.Stat(%q): %v", filePath, err)
	}
	dir := t.TempDir()

	f := &FileNode{
		entry:       entry,
		reader:      reader,
		tempDir:     dir,
		layerDigest: "sha256:fallback",
		sharedCache: &fakeSharedCache{
			prepareFn: func(digest string, cachedPath string, fill func(w io.Writer) (int64, error)) (string, int64, error) {
				if digest != "sha256:fallback" {
					t.Fatalf("unexpected digest: %q", digest)
				}
				if cachedPath != filePath {
					t.Fatalf("unexpected cached path: %q", cachedPath)
				}
				return filepath.Join(dir, "evicted-from-shared-cache.spill"), int64(len(content)), nil
			},
		},
	}

	fd, n, err := f.prepareTempFile()
	if err != nil {
		t.Fatalf("prepareTempFile fallback: %v", err)
	}
	defer fd.Close()
	if n != int64(len(content)) {
		t.Fatalf("size=%d, want %d", n, len(content))
	}
	got, err := io.ReadAll(fd)
	if err != nil {
		t.Fatalf("read fallback spill file: %v", err)
	}
	if string(got) != content {
		t.Fatalf("fallback content=%q, want %q", string(got), content)
	}
	if _, statErr := os.Stat(filepath.Join(dir, spillFileName(entry))); statErr != nil {
		t.Fatalf("expected local spill file created, stat error: %v", statErr)
	}
}

func newReaderWithSingleFile(t testing.TB, name string, data []byte) *layerformat.Reader {
	t.Helper()

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("write tar payload: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write gzip payload: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	var archive bytes.Buffer
	if err := layerformat.ConvertTarGzipToArchive(bytes.NewReader(gzBuf.Bytes()), &archive); err != nil {
		t.Fatalf("convert layer archive: %v", err)
	}
	reader, err := layerformat.NewReader(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("open layer archive reader: %v", err)
	}
	return reader
}

func newV2ReaderWithSingleFile(t testing.TB, name string, data []byte) *layerformat.Reader {
	t.Helper()

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("write tar payload: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write gzip payload: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	var archive bytes.Buffer
	if err := layerformat.ConvertTarGzipToArchiveV2(bytes.NewReader(gzBuf.Bytes()), &archive); err != nil {
		t.Fatalf("convert layer archive v2: %v", err)
	}
	reader, err := layerformat.NewReader(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("open layer archive reader: %v", err)
	}
	return reader
}

func TestV2FileNodeDirectRead(t *testing.T) {
	t.Parallel()

	const (
		filePath = "test.txt"
		content  = "hello-direct-read-from-archive"
	)
	reader := newV2ReaderWithSingleFile(t, filePath, []byte(content))

	if reader.FormatVersion() != layerformat.FormatV2 {
		t.Fatalf("expected FormatV2, got %d", reader.FormatVersion())
	}

	section, err := reader.OpenFileSection(filePath)
	if err != nil {
		t.Fatalf("OpenFileSection() error = %v", err)
	}

	node := &FileNode{
		entry:         layerformat.Entry{Path: filePath, Type: layerformat.EntryTypeFile, Mode: 0o644},
		reader:        reader,
		directSection: &section,
	}

	// Test sequential read
	dest := make([]byte, len(content))
	ctx := context.TODO()
	result, errno := node.Read(ctx, nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read() errno = %d", errno)
	}
	buf, status := result.Bytes(dest)
	if status != fuse.OK {
		t.Fatalf("ReadResult.Bytes() status = %v", status)
	}
	if string(buf) != content {
		t.Fatalf("Read() = %q, want %q", string(buf), content)
	}

	// Test offset read
	dest2 := make([]byte, 10)
	result2, errno2 := node.Read(ctx, nil, dest2, 6)
	if errno2 != 0 {
		t.Fatalf("Read(off=6) errno = %d", errno2)
	}
	buf2, status2 := result2.Bytes(dest2)
	if status2 != fuse.OK {
		t.Fatalf("ReadResult.Bytes() status = %v", status2)
	}
	if string(buf2) != "direct-rea" {
		t.Fatalf("Read(off=6) = %q, want %q", string(buf2), "direct-rea")
	}

	// Test read past end
	dest3 := make([]byte, 10)
	result3, errno3 := node.Read(ctx, nil, dest3, int64(len(content)+10))
	if errno3 != 0 {
		t.Fatalf("Read(past end) errno = %d", errno3)
	}
	buf3, _ := result3.Bytes(dest3)
	if len(buf3) != 0 {
		t.Fatalf("Read(past end) should return empty, got %d bytes", len(buf3))
	}
}

func TestV2FileNodeNoSpillFileCreated(t *testing.T) {
	t.Parallel()

	const (
		filePath = "nospill.txt"
		content  = "no-spill-file-should-be-created"
	)
	reader := newV2ReaderWithSingleFile(t, filePath, []byte(content))

	section, err := reader.OpenFileSection(filePath)
	if err != nil {
		t.Fatalf("OpenFileSection() error = %v", err)
	}

	dir := t.TempDir()
	node := &FileNode{
		entry:         layerformat.Entry{Path: filePath, Type: layerformat.EntryTypeFile, Mode: 0o644, UncompressedSize: int64(len(content))},
		reader:        reader,
		tempDir:       dir,
		directSection: &section,
	}

	// Perform a read
	dest := make([]byte, len(content))
	_, errno := node.Read(context.TODO(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read() errno = %d", errno)
	}

	// Verify no spill files were created
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			t.Fatalf("unexpected spill file created: %s", e.Name())
		}
	}
}

func TestV2FileNodeGetattr(t *testing.T) {
	t.Parallel()

	const (
		filePath = "attr.txt"
		content  = "file-content-for-getattr"
	)
	reader := newV2ReaderWithSingleFile(t, filePath, []byte(content))

	section, err := reader.OpenFileSection(filePath)
	if err != nil {
		t.Fatalf("OpenFileSection() error = %v", err)
	}

	node := &FileNode{
		entry:         layerformat.Entry{Path: filePath, Type: layerformat.EntryTypeFile, Mode: 0o644, UncompressedSize: int64(len(content))},
		reader:        reader,
		directSection: &section,
	}

	out := &fuse.AttrOut{}
	errno := node.Getattr(context.TODO(), nil, out)
	if errno != 0 {
		t.Fatalf("Getattr() errno = %d", errno)
	}
	if out.Size != uint64(len(content)) {
		t.Fatalf("Getattr() Size=%d, want %d", out.Size, len(content))
	}
}
