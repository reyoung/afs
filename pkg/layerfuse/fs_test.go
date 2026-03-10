package layerfuse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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

func newReaderWithSingleFile(t *testing.T, name string, data []byte) *layerformat.Reader {
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
