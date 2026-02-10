package layerfuse

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
)

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
