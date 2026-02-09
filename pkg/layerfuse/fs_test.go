package layerfuse

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
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
