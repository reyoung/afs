package spillcache

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type zeroSource struct{}

func (zeroSource) Int63() int64 { return 0 }

func (zeroSource) Seed(int64) {}

func TestCacheStoreAcquireCommitAndHit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := newCacheStore(dir, 1<<20)
	if err != nil {
		t.Fatalf("newCacheStore: %v", err)
	}

	hitPath, hitSize, lease, err := store.acquire(cacheKey{Digest: "sha256:abc", FilePath: "/a/b"})
	if err != nil {
		t.Fatalf("acquire miss: %v", err)
	}
	if hitPath != "" || hitSize != 0 || lease == nil {
		t.Fatalf("unexpected acquire miss response: hitPath=%q hitSize=%d lease=%v", hitPath, hitSize, lease)
	}
	if err := os.WriteFile(lease.TempPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	cachePath, size, err := store.commit(lease.Token)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if size != 5 {
		t.Fatalf("size=%d, want 5", size)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("stat cache path: %v", err)
	}

	hitPath, hitSize, lease, err = store.acquire(cacheKey{Digest: "sha256:abc", FilePath: "/a/b"})
	if err != nil {
		t.Fatalf("acquire hit: %v", err)
	}
	if lease != nil {
		t.Fatalf("expected nil lease on cache hit")
	}
	if hitPath != cachePath || hitSize != 5 {
		t.Fatalf("hit mismatch path=%q size=%d", hitPath, hitSize)
	}
}

func TestCacheStoreEvictionApproxLRU(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := newCacheStore(dir, 10)
	if err != nil {
		t.Fatalf("newCacheStore: %v", err)
	}
	store.rnd = rand.New(rand.NewSource(1))

	writeEntry := func(digest, p, content string) string {
		t.Helper()
		_, _, lease, acqErr := store.acquire(cacheKey{Digest: digest, FilePath: p})
		if acqErr != nil {
			t.Fatalf("acquire %s: %v", p, acqErr)
		}
		if err := os.WriteFile(lease.TempPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write temp %s: %v", p, err)
		}
		cachePath, _, commitErr := store.commit(lease.Token)
		if commitErr != nil {
			t.Fatalf("commit %s: %v", p, commitErr)
		}
		return cachePath
	}

	_ = writeEntry("sha256:1", "/f1", "aaaa") // 4
	_ = writeEntry("sha256:2", "/f2", "bbbb") // 8
	_ = writeEntry("sha256:3", "/f3", "cccc") // 12 -> evict one

	if store.totalBytes > store.maxBytes {
		t.Fatalf("cache bytes=%d exceeds max=%d", store.totalBytes, store.maxBytes)
	}
	if len(store.entries) == 0 {
		t.Fatalf("expected non-empty entries after eviction")
	}
}

func TestClientPrepareSharedHitAcrossProcesses(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sock := filepath.Join(dir, "daemon.sock")
	go func() {
		_ = RunServer(ServerConfig{CacheDir: dir, SockPath: sock, MaxBytes: 1 << 20})
	}()
	client := NewClient(sock, 3*time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := client.Ping(); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := client.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	calls := 0
	fill := func(w io.Writer) (int64, error) {
		calls++
		n, err := w.Write([]byte("payload"))
		return int64(n), err
	}

	p1, s1, err := client.Prepare("sha256:abc", "/x", fill)
	if err != nil {
		t.Fatalf("prepare first: %v", err)
	}
	p2, s2, err := client.Prepare("sha256:abc", "/x", fill)
	if err != nil {
		t.Fatalf("prepare second: %v", err)
	}
	if calls != 1 {
		t.Fatalf("fill calls=%d, want 1", calls)
	}
	if p1 != p2 || s1 != s2 {
		t.Fatalf("cache path/size mismatch p1=%q p2=%q s1=%d s2=%d", p1, p2, s1, s2)
	}
}

func TestCacheStoreEvictionFallsBackWhenApproxSamplerMisses(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := newCacheStore(dir, 8)
	if err != nil {
		t.Fatalf("newCacheStore: %v", err)
	}
	store.rnd = rand.New(zeroSource{})

	protectedKey := cacheKey{Digest: "sha256:a", FilePath: "/a"}.keyString()
	victimKey := cacheKey{Digest: "sha256:b", FilePath: "/b"}.keyString()
	protectedPath := filepath.Join(dir, "protected.spill")
	victimPath := filepath.Join(dir, "victim.spill")
	if err := os.WriteFile(protectedPath, []byte("12345678"), 0o644); err != nil {
		t.Fatalf("write protected: %v", err)
	}
	if err := os.WriteFile(victimPath, []byte("abcdefgh"), 0o644); err != nil {
		t.Fatalf("write victim: %v", err)
	}
	store.entries[protectedKey] = &entry{
		Digest:         "sha256:a",
		FilePath:       "/a",
		Hash:           "a",
		DataFile:       protectedPath,
		Size:           8,
		LastAccessUnix: time.Now().Unix(),
	}
	store.entries[victimKey] = &entry{
		Digest:         "sha256:b",
		FilePath:       "/b",
		Hash:           "b",
		DataFile:       victimPath,
		Size:           8,
		LastAccessUnix: time.Now().Unix() - 1,
	}
	store.totalBytes = 16

	store.mu.Lock()
	err = store.evictIfNeededLocked(protectedKey)
	store.mu.Unlock()
	if err != nil {
		t.Fatalf("evictIfNeededLocked: %v", err)
	}
	if store.totalBytes > store.maxBytes {
		t.Fatalf("cache bytes=%d exceeds max=%d", store.totalBytes, store.maxBytes)
	}
	if _, ok := store.entries[protectedKey]; !ok {
		t.Fatalf("protected key should not be evicted when another entry exists")
	}
	if _, ok := store.entries[victimKey]; ok {
		t.Fatalf("victim key should be evicted")
	}
	if _, statErr := os.Stat(victimPath); !os.IsNotExist(statErr) {
		t.Fatalf("victim file should be removed, stat err=%v", statErr)
	}
}
