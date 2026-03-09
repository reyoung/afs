package layerfuse

import (
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
