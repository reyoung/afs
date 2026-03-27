package pagecache

import (
	"sync"
	"testing"
)

func TestPageIndex_PutAndGet(t *testing.T) {
	idx := NewPageIndex()
	key := MakePageKey("sha256:abc123", 0)

	entry := &CacheEntry{
		ChunkID:   1,
		SlabIndex: 0,
		SlabClass: 2,
	}
	idx.Put(key, entry)

	got, ok := idx.Get(key)
	if !ok {
		t.Fatal("expected to find entry")
	}
	if got.ChunkID != 1 || got.SlabClass != 2 {
		t.Fatalf("unexpected entry: %+v", got)
	}
}

func TestPageIndex_GetMissing(t *testing.T) {
	idx := NewPageIndex()
	key := MakePageKey("nonexistent", 99)

	_, ok := idx.Get(key)
	if ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestPageIndex_Delete(t *testing.T) {
	idx := NewPageIndex()
	key := MakePageKey("sha256:abc123", 0)

	idx.Put(key, &CacheEntry{ChunkID: 1})
	idx.Delete(key)

	_, ok := idx.Get(key)
	if ok {
		t.Fatal("expected entry to be deleted")
	}
}

func TestPageIndex_Len(t *testing.T) {
	idx := NewPageIndex()

	if idx.Len() != 0 {
		t.Fatalf("expected len 0, got %d", idx.Len())
	}

	idx.Put(MakePageKey("a", 0), &CacheEntry{})
	idx.Put(MakePageKey("b", 1), &CacheEntry{})

	if idx.Len() != 2 {
		t.Fatalf("expected len 2, got %d", idx.Len())
	}
}

func TestPageIndex_Overwrite(t *testing.T) {
	idx := NewPageIndex()
	key := MakePageKey("digest", 5)

	idx.Put(key, &CacheEntry{ChunkID: 1})
	idx.Put(key, &CacheEntry{ChunkID: 2})

	got, ok := idx.Get(key)
	if !ok {
		t.Fatal("expected to find entry after overwrite")
	}
	if got.ChunkID != 2 {
		t.Fatalf("expected ChunkID 2 after overwrite, got %d", got.ChunkID)
	}

	if idx.Len() != 1 {
		t.Fatalf("expected len 1 after overwrite, got %d", idx.Len())
	}
}

func TestMakePageKey_Deterministic(t *testing.T) {
	k1 := MakePageKey("sha256:deadbeef", 42)
	k2 := MakePageKey("sha256:deadbeef", 42)

	if k1 != k2 {
		t.Fatal("expected identical keys for same inputs")
	}
}

func TestMakePageKey_DifferentDigests(t *testing.T) {
	k1 := MakePageKey("sha256:aaa", 0)
	k2 := MakePageKey("sha256:bbb", 0)

	if k1 == k2 {
		t.Fatal("expected different keys for different digests")
	}
}

func TestMakePageKey_DifferentPages(t *testing.T) {
	k1 := MakePageKey("sha256:same", 0)
	k2 := MakePageKey("sha256:same", 1)

	if k1 == k2 {
		t.Fatal("expected different keys for different page IDs")
	}
}

func TestHashDigest_Consistency(t *testing.T) {
	h1 := hashDigest("hello")
	h2 := hashDigest("hello")
	if h1 != h2 {
		t.Fatal("expected consistent hash")
	}

	h3 := hashDigest("world")
	if h1 == h3 {
		t.Fatal("expected different hashes for different inputs")
	}
}

func TestPageIndex_ConcurrentAccess(t *testing.T) {
	idx := NewPageIndex()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := MakePageKey("digest", uint64(i))
			idx.Put(key, &CacheEntry{ChunkID: uint32(i)})
			idx.Get(key)
			idx.Len()
		}(i)
	}
	wg.Wait()

	if idx.Len() != 100 {
		t.Fatalf("expected 100 entries, got %d", idx.Len())
	}
}

func TestPageIndex_AcquireReleaseAndDeleteIfUnpinned(t *testing.T) {
	idx := NewPageIndex()
	key := MakePageKey("digest", 7)
	idx.Put(key, &CacheEntry{ChunkID: 1})

	entry, ok := idx.Acquire(key)
	if !ok {
		t.Fatal("Acquire() miss, want hit")
	}
	if entry.RefCount != 1 {
		t.Fatalf("RefCount after Acquire = %d, want 1", entry.RefCount)
	}

	if _, ok := idx.DeleteIfUnpinned(key); ok {
		t.Fatal("DeleteIfUnpinned() succeeded while entry pinned")
	}
	if !idx.IsPinned(key) {
		t.Fatal("IsPinned() = false, want true")
	}

	if !idx.Release(key) {
		t.Fatal("Release() = false, want true")
	}
	if got := idx.RefCountForKey(key); got != 0 {
		t.Fatalf("RefCount after Release = %d, want 0", got)
	}

	deleted, ok := idx.DeleteIfUnpinned(key)
	if !ok {
		t.Fatal("DeleteIfUnpinned() miss after Release")
	}
	if deleted.ChunkID != 1 {
		t.Fatalf("deleted ChunkID = %d, want 1", deleted.ChunkID)
	}
}
