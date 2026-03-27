package filecache

import (
	"bytes"
	"io"
	"testing"
)

func TestStoreReadThroughCachesSmallFile(t *testing.T) {
	store, err := NewStore(t.TempDir(), 1<<20, 1<<20)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	payload := []byte("small-elf-payload")
	buf := make([]byte, len(payload))
	n, err := store.ReadThrough(bytes.NewReader(payload), "sha256:test", int64(len(payload)), buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadThrough() err = %v", err)
	}
	if got := string(buf[:n]); got != string(payload) {
		t.Fatalf("ReadThrough() = %q, want %q", got, string(payload))
	}
	if store.MissCount() != 1 {
		t.Fatalf("MissCount = %d, want 1", store.MissCount())
	}

	buf2 := make([]byte, len(payload))
	n, err = store.ReadThrough(bytes.NewReader([]byte("xxxxxxxxxxxxxxxxx")), "sha256:test", int64(len(payload)), buf2, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("second ReadThrough() err = %v", err)
	}
	if got := string(buf2[:n]); got != string(payload) {
		t.Fatalf("cached ReadThrough() = %q, want %q", got, string(payload))
	}
	if store.HitCount() != 1 {
		t.Fatalf("HitCount = %d, want 1", store.HitCount())
	}
}

func TestStoreSkipsLargeFile(t *testing.T) {
	store, err := NewStore(t.TempDir(), 1<<20, 4)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	payload := []byte("too-large")
	buf := make([]byte, len(payload))
	n, err := store.ReadThrough(bytes.NewReader(payload), "sha256:large", int64(len(payload)), buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadThrough() err = %v", err)
	}
	if got := string(buf[:n]); got != string(payload) {
		t.Fatalf("ReadThrough() = %q, want %q", got, string(payload))
	}
	if store.MissCount() != 0 || store.HitCount() != 0 {
		t.Fatalf("cache counters = (%d,%d), want (0,0)", store.HitCount(), store.MissCount())
	}
}
