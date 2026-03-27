package pagecache

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// Store is an in-process page cache backed by slab-allocated chunk files on disk.
// It is safe for concurrent use from multiple goroutines.
type Store struct {
	slab     *SlabAllocator
	index    *PageIndex
	eviction *evictionPolicy

	// chunkFDs caches open file descriptors for chunk files to avoid per-read open/close.
	chunkFDs map[uint32]*os.File
	fdMu     sync.RWMutex

	hitCount  atomic.Int64
	missCount atomic.Int64
}

// NewStore creates a new in-process page cache store.
func NewStore(cacheDir string, maxBytes int64) (*Store, error) {
	slab, err := NewSlabAllocator(cacheDir, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("init slab allocator: %w", err)
	}

	maxPages := int(maxBytes / (64 << 10))
	if maxPages < 1024 {
		maxPages = 1024
	}

	return &Store{
		slab:     slab,
		index:    NewPageIndex(),
		eviction: newEvictionPolicy(maxPages),
		chunkFDs: make(map[uint32]*os.File),
	}, nil
}

// Close closes all cached file descriptors.
func (s *Store) Close() error {
	s.fdMu.Lock()
	defer s.fdMu.Unlock()
	for _, f := range s.chunkFDs {
		_ = f.Close()
	}
	s.chunkFDs = nil
	return nil
}

// choosePageSize selects an appropriate page size based on file size.
// Small files use smaller pages; large files use 256KB pages.
func choosePageSize(fileSize int64) int {
	switch {
	case fileSize <= 64<<10:
		return 64 << 10 // 64KB
	case fileSize <= 1<<20:
		return 64 << 10 // 64KB
	default:
		return 256 << 10 // 256KB
	}
}

// ReadThrough reads len(dest) bytes at offset off from a file identified by digest/fileSize.
// On cache hit, data is read from local chunk files. On miss, data is fetched from underlying,
// cached, and returned.
func (s *Store) ReadThrough(underlying io.ReaderAt, digest string, fileSize int64, dest []byte, off int64) (int, error) {
	if off >= fileSize {
		return 0, io.EOF
	}

	remain := fileSize - off
	if remain > int64(len(dest)) {
		remain = int64(len(dest))
	}
	if remain <= 0 {
		return 0, io.EOF
	}

	pageSize := choosePageSize(fileSize)
	totalRead := 0

	for totalRead < int(remain) {
		pageID := uint64(off / int64(pageSize))
		pageOff := int(off % int64(pageSize))

		// How much of this page do we need?
		wantFromPage := int(remain) - totalRead
		pageRemain := pageSize - pageOff
		if wantFromPage > pageRemain {
			wantFromPage = pageRemain
		}
		if off+int64(wantFromPage) > fileSize {
			wantFromPage = int(fileSize - off)
		}

		n, err := s.readPage(underlying, digest, fileSize, pageSize, pageID, pageOff, dest[totalRead:totalRead+wantFromPage])
		totalRead += n
		off += int64(n)
		if err != nil {
			if totalRead > 0 {
				return totalRead, nil
			}
			return totalRead, err
		}
	}

	if totalRead == 0 && off >= fileSize {
		return 0, io.EOF
	}
	return totalRead, nil
}

// readPage reads a slice from a single page, using cache when possible.
func (s *Store) readPage(underlying io.ReaderAt, digest string, fileSize int64, pageSize int, pageID uint64, pageOffset int, dst []byte) (int, error) {
	key := MakePageKey(digest, pageID)

	// Try cache hit
	entry, ok := s.index.Get(key)
	if ok {
		s.hitCount.Add(1)
		s.eviction.Access(key)

		offset := s.slab.SlabOffset(entry.SlabClass, entry.SlabIndex) + int64(pageOffset)
		n, err := s.preadChunk(entry.ChunkID, dst, offset)
		if err == nil {
			return n, nil
		}
		// pread failed (shouldn't happen), fall through to miss
	}

	s.missCount.Add(1)

	// Cache miss: read full page from underlying
	pageStart := int64(pageID) * int64(pageSize)
	pageEnd := pageStart + int64(pageSize)
	if pageEnd > fileSize {
		pageEnd = fileSize
	}
	actualPageSize := int(pageEnd - pageStart)

	pageBuf := make([]byte, actualPageSize)
	n, err := underlying.ReadAt(pageBuf, pageStart)
	if err != nil && err != io.EOF {
		return 0, err
	}
	pageBuf = pageBuf[:n]

	// Write to cache (best effort)
	s.putPage(key, pageBuf)

	// Copy requested portion
	if pageOffset >= len(pageBuf) {
		return 0, io.EOF
	}
	copied := copy(dst, pageBuf[pageOffset:])
	return copied, nil
}

// putPage writes a page to the cache.
func (s *Store) putPage(key pageKey, data []byte) {
	// Check if already exists
	if _, ok := s.index.Get(key); ok {
		return
	}

	slabClass := SlabClassForSize(len(data))
	if slabClass < 0 {
		return // too large for any slab
	}

	chunkID, slabIndex, err := s.slab.Allocate(slabClass)
	if err != nil {
		// Try eviction
		chunkID, slabIndex, err = s.allocateWithEviction(slabClass)
		if err != nil {
			return // no space
		}
	}

	// Write data to chunk file
	offset := s.slab.SlabOffset(slabClass, slabIndex)
	if writeErr := s.pwriteChunk(chunkID, data, offset); writeErr != nil {
		s.slab.Free(chunkID, slabIndex)
		return
	}

	// Update index and eviction
	entry := &CacheEntry{
		ChunkID:   chunkID,
		SlabIndex: slabIndex,
		SlabClass: slabClass,
		DataSize:  uint32(len(data)),
	}
	s.index.Put(key, entry)

	evicted := s.eviction.Add(key)
	if evicted != nil {
		s.evictEntry(*evicted)
	}
}

// evictEntry removes an entry from the cache.
func (s *Store) evictEntry(key pageKey) {
	entry, ok := s.index.Get(key)
	if !ok {
		return
	}
	s.slab.Free(entry.ChunkID, entry.SlabIndex)
	s.index.Delete(key)
}

// allocateWithEviction tries to make space by evicting entries.
func (s *Store) allocateWithEviction(slabClass int) (uint32, int, error) {
	for attempts := 0; attempts < 64; attempts++ {
		victim := s.eviction.FindVictim(func(key pageKey) bool {
			return false
		})
		if victim == nil {
			break
		}

		s.evictEntry(*victim)
		s.eviction.Remove(*victim)

		chunkID, slabIndex, err := s.slab.Allocate(slabClass)
		if err == nil {
			return chunkID, slabIndex, nil
		}
	}
	return 0, 0, fmt.Errorf("no space after eviction attempts")
}

// preadChunk reads from a chunk file using a cached fd.
func (s *Store) preadChunk(chunkID uint32, dst []byte, offset int64) (int, error) {
	f, err := s.getChunkFD(chunkID)
	if err != nil {
		return 0, err
	}
	return f.ReadAt(dst, offset)
}

// pwriteChunk writes to a chunk file using a cached fd.
func (s *Store) pwriteChunk(chunkID uint32, data []byte, offset int64) error {
	f, err := s.getChunkFD(chunkID)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, offset)
	return err
}

// getChunkFD returns a cached file descriptor for the chunk, opening it if needed.
func (s *Store) getChunkFD(chunkID uint32) (*os.File, error) {
	// Fast path: read lock
	s.fdMu.RLock()
	if f, ok := s.chunkFDs[chunkID]; ok {
		s.fdMu.RUnlock()
		return f, nil
	}
	s.fdMu.RUnlock()

	// Slow path: write lock
	s.fdMu.Lock()
	defer s.fdMu.Unlock()

	// Double check
	if f, ok := s.chunkFDs[chunkID]; ok {
		return f, nil
	}

	path := s.slab.ChunkFilePath(chunkID)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open chunk file %s: %w", path, err)
	}
	s.chunkFDs[chunkID] = f
	return f, nil
}

// HitCount returns the number of cache hits.
func (s *Store) HitCount() int64 { return s.hitCount.Load() }

// MissCount returns the number of cache misses.
func (s *Store) MissCount() int64 { return s.missCount.Load() }
