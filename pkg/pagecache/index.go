package pagecache

import (
	"hash/fnv"
	"sync"
)

// CacheEntry represents a cached page in the slab allocator.
type CacheEntry struct {
	ChunkID   uint32
	SlabIndex int
	SlabClass int
	DataSize  uint32
	RefCount  int32
}

// PageIndex is a thread-safe in-memory index mapping page keys to cache entries.
type PageIndex struct {
	entries map[pageKey]*CacheEntry
	mu      sync.RWMutex
}

// NewPageIndex creates a new, empty PageIndex.
func NewPageIndex() *PageIndex {
	return &PageIndex{
		entries: make(map[pageKey]*CacheEntry),
	}
}

// Get retrieves the cache entry for the given key. Returns (entry, true) if
// found, or (nil, false) otherwise.
func (idx *PageIndex) Get(key pageKey) (*CacheEntry, bool) {
	idx.mu.RLock()
	e, ok := idx.entries[key]
	idx.mu.RUnlock()
	return e, ok
}

// Put inserts or replaces the cache entry for the given key.
func (idx *PageIndex) Put(key pageKey, entry *CacheEntry) {
	idx.mu.Lock()
	idx.entries[key] = entry
	idx.mu.Unlock()
}

// Delete removes the cache entry for the given key.
func (idx *PageIndex) Delete(key pageKey) {
	idx.mu.Lock()
	delete(idx.entries, key)
	idx.mu.Unlock()
}

// Acquire increments the refcount for the given key and returns the entry.
// While an entry is acquired, it must not be evicted.
func (idx *PageIndex) Acquire(key pageKey) (*CacheEntry, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entry, ok := idx.entries[key]
	if !ok {
		return nil, false
	}
	entry.RefCount++
	return entry, true
}

// Release decrements the refcount for the given key.
func (idx *PageIndex) Release(key pageKey) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entry, ok := idx.entries[key]
	if !ok {
		return false
	}
	if entry.RefCount > 0 {
		entry.RefCount--
	}
	return true
}

// DeleteIfUnpinned removes the entry only when no active references remain.
func (idx *PageIndex) DeleteIfUnpinned(key pageKey) (*CacheEntry, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entry, ok := idx.entries[key]
	if !ok {
		return nil, false
	}
	if entry.RefCount > 0 {
		return nil, false
	}
	delete(idx.entries, key)
	return entry, true
}

// IsPinned reports whether the entry currently has active references.
func (idx *PageIndex) IsPinned(key pageKey) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entry, ok := idx.entries[key]
	return ok && entry.RefCount > 0
}

// RefCountForKey reports the current refcount for tests and debugging.
func (idx *PageIndex) RefCountForKey(key pageKey) int32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entry, ok := idx.entries[key]
	if !ok {
		return 0
	}
	return entry.RefCount
}

// Len returns the number of entries in the index.
func (idx *PageIndex) Len() int {
	idx.mu.RLock()
	n := len(idx.entries)
	idx.mu.RUnlock()
	return n
}

// hashDigest computes a uint64 hash of a digest string.
func hashDigest(digest string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(digest))
	return h.Sum64()
}

// MakePageKey constructs a pageKey from a digest string and page ID.
func MakePageKey(digest string, pageID uint64) pageKey {
	return pageKey{
		digestHash: hashDigest(digest),
		pageID:     pageID,
	}
}
