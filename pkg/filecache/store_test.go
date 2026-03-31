package filecache

import (
	"bytes"
	"io"
	"os"
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

func TestStorePinnedEntrySkipsEviction(t *testing.T) {
	store, err := NewStore(t.TempDir(), 8, 1<<20)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	payloadA := []byte("aaaaaaaa")
	bufA := make([]byte, len(payloadA))
	if _, err := store.ReadThrough(bytes.NewReader(payloadA), "sha256:a", int64(len(payloadA)), bufA, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadThrough(a) err = %v", err)
	}

	entryA, ok := store.entries["sha256:a"]
	if !ok {
		t.Fatalf("missing cache entry for digest a")
	}
	if !store.pinEntry(entryA) {
		t.Fatalf("expected to pin entry a")
	}
	defer store.unpinEntry(entryA)

	payloadB := []byte("bbbbbbbb")
	bufB := make([]byte, len(payloadB))
	if _, err := store.ReadThrough(bytes.NewReader(payloadB), "sha256:b", int64(len(payloadB)), bufB, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadThrough(b) err = %v", err)
	}

	if _, ok := store.entries["sha256:a"]; !ok {
		t.Fatalf("pinned entry a should not be evicted")
	}
}

func TestStoreFallsBackToDirectReadWhenCacheFDIsInvalid(t *testing.T) {
	store, err := NewStore(t.TempDir(), 1<<20, 1<<20)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	payload := []byte("small-elf-payload")
	buf := make([]byte, len(payload))
	if _, err := store.ReadThrough(bytes.NewReader(payload), "sha256:test", int64(len(payload)), buf, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadThrough() err = %v", err)
	}

	entry := store.entries["sha256:test"]
	if entry == nil || entry.fd == nil {
		t.Fatalf("expected cache entry fd")
	}
	if err := entry.fd.Close(); err != nil {
		t.Fatalf("close cache fd: %v", err)
	}
	entry.fd = nil
	if err := os.Remove(entry.path); err != nil {
		t.Fatalf("remove cache file: %v", err)
	}

	buf2 := make([]byte, len(payload))
	n, err := store.ReadThrough(bytes.NewReader(payload), "sha256:test", int64(len(payload)), buf2, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadThrough fallback err = %v", err)
	}
	if got := string(buf2[:n]); got != string(payload) {
		t.Fatalf("fallback ReadThrough() = %q, want %q", got, string(payload))
	}
}
