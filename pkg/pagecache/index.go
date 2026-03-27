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
