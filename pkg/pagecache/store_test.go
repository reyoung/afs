package pagecache

import (
	"bytes"
	"syscall"
	"testing"
)

func TestStoreAcquireReadFDPinsUntilRelease(t *testing.T) {
	t.Parallel()

	store, err := NewStore(t.TempDir(), 8*ChunkSize)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer store.Close()

	payload := bytes.Repeat([]byte("abcdef0123456789"), 2048)
	const digest = "sha256:test-read-fd"

	warm := make([]byte, 4096)
	if n, err := store.ReadThrough(bytes.NewReader(payload), digest, int64(len(payload)), warm, 0); err != nil || n != len(warm) {
		t.Fatalf("ReadThrough() n=%d err=%v, want %d nil", n, err, len(warm))
	}

	readFD, ok := store.AcquireReadFD(digest, int64(len(payload)), 0, len(warm))
	if !ok {
		t.Fatal("AcquireReadFD() miss, want hit")
	}
	key := MakePageKey(digest, 0)
	if got := store.index.RefCountForKey(key); got != 1 {
		t.Fatalf("RefCount while pinned = %d, want 1", got)
	}

	buf := make([]byte, len(warm))
	n, err := syscall.Pread(int(readFD.FD), buf, readFD.Offset)
	if err != nil {
		t.Fatalf("Pread() error = %v", err)
	}
	if !bytes.Equal(buf[:n], payload[:n]) {
		t.Fatal("Pread() returned unexpected bytes")
	}

	store.evictEntry(key)
	if _, ok := store.index.Get(key); !ok {
		t.Fatal("entry evicted while pinned")
	}

	readFD.Release()
	if got := store.index.RefCountForKey(key); got != 0 {
		t.Fatalf("RefCount after Release = %d, want 0", got)
	}

	store.evictEntry(key)
	if _, ok := store.index.Get(key); ok {
		t.Fatal("entry still present after eviction")
	}
}

func TestStoreAcquireReadFDCrossPageFallsBack(t *testing.T) {
	t.Parallel()

	store, err := NewStore(t.TempDir(), 8*ChunkSize)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer store.Close()

	payload := bytes.Repeat([]byte("x"), 128<<10)
	const digest = "sha256:test-cross-page"

	warm := make([]byte, 80<<10)
	if _, err := store.ReadThrough(bytes.NewReader(payload), digest, int64(len(payload)), warm, 0); err != nil {
		t.Fatalf("ReadThrough() error = %v", err)
	}

	if readFD, ok := store.AcquireReadFD(digest, int64(len(payload)), 0, len(warm)); ok {
		readFD.Release()
		t.Fatal("AcquireReadFD() = hit for cross-page read, want miss")
	}
}
