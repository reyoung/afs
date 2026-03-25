// Package pagecache implements a slab allocator backed by pre-allocated chunk
// files on disk. Each chunk is a 2 MB file that stores fixed-size slabs of a
// single slab class. Free/used tracking is done with per-chunk bitmaps,
// giving O(1) allocation and deallocation.
package pagecache

import (
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
)

// ChunkSize is the size of every chunk file (2 MB).
const ChunkSize = 2 << 20 // 2 097 152 bytes

// SlabClass describes one size class.
type SlabClass struct {
	SlabSize      int
	SlabsPerChunk int
}

// SlabClasses lists the 11 supported slab sizes from 2 KB to 2 MB.
var SlabClasses = [11]SlabClass{
	{SlabSize: 2 << 10, SlabsPerChunk: ChunkSize / (2 << 10)},     // 2 KB  → 1024 slabs
	{SlabSize: 4 << 10, SlabsPerChunk: ChunkSize / (4 << 10)},     // 4 KB  → 512
	{SlabSize: 8 << 10, SlabsPerChunk: ChunkSize / (8 << 10)},     // 8 KB  → 256
	{SlabSize: 16 << 10, SlabsPerChunk: ChunkSize / (16 << 10)},   // 16 KB → 128
	{SlabSize: 32 << 10, SlabsPerChunk: ChunkSize / (32 << 10)},   // 32 KB → 64
	{SlabSize: 64 << 10, SlabsPerChunk: ChunkSize / (64 << 10)},   // 64 KB → 32
	{SlabSize: 128 << 10, SlabsPerChunk: ChunkSize / (128 << 10)}, // 128 KB → 16
	{SlabSize: 256 << 10, SlabsPerChunk: ChunkSize / (256 << 10)}, // 256 KB → 8
	{SlabSize: 512 << 10, SlabsPerChunk: ChunkSize / (512 << 10)}, // 512 KB → 4
	{SlabSize: 1 << 20, SlabsPerChunk: ChunkSize / (1 << 20)},     // 1 MB  → 2
	{SlabSize: 2 << 20, SlabsPerChunk: 1},                         // 2 MB  → 1
}

// SlabClassForSize returns the index of the smallest slab class whose
// SlabSize is >= size. It returns -1 if size exceeds the largest class.
func SlabClassForSize(size int) int {
	for i, sc := range SlabClasses {
		if sc.SlabSize >= size {
			return i
		}
	}
	return -1
}

// chunkMeta holds the in-memory bookkeeping for a single chunk file.
type chunkMeta struct {
	chunkID   uint32
	slabClass int // -1 when the chunk is unassigned (in the free pool)
	usedSlabs int
	// freeBitmap has one bit per slab; bit set means the slab is FREE.
	freeBitmap []uint64
}

// SlabAllocator manages a pool of chunk files and hands out fixed-size slabs.
type SlabAllocator struct {
	cacheDir  string
	maxChunks int
	chunks    []chunkMeta

	// partialChunks[class] lists chunk IDs that belong to this class and
	// still have at least one free slab.
	partialChunks [11][]uint32
	// freeChunks lists chunk IDs that are not assigned to any class.
	freeChunks []uint32

	mu sync.Mutex
}

// NewSlabAllocator creates the chunk directory and pre-allocates chunk files.
func NewSlabAllocator(cacheDir string, maxBytes int64) (*SlabAllocator, error) {
	chunkDir := filepath.Join(cacheDir, "chunks")
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return nil, fmt.Errorf("pagecache: mkdir %s: %w", chunkDir, err)
	}

	maxChunks := int(maxBytes / ChunkSize)
	if maxChunks <= 0 {
		return nil, fmt.Errorf("pagecache: maxBytes (%d) too small for even one chunk", maxBytes)
	}

	sa := &SlabAllocator{
		cacheDir:  cacheDir,
		maxChunks: maxChunks,
		chunks:    make([]chunkMeta, maxChunks),
	}

	// Pre-allocate chunk files and populate the free pool.
	sa.freeChunks = make([]uint32, 0, maxChunks)
	for i := 0; i < maxChunks; i++ {
		id := uint32(i)
		path := sa.ChunkFilePath(id)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, fmt.Errorf("pagecache: create chunk %d: %w", i, err)
		}
		if err := f.Truncate(ChunkSize); err != nil {
			f.Close()
			return nil, fmt.Errorf("pagecache: truncate chunk %d: %w", i, err)
		}
		f.Close()

		sa.chunks[i] = chunkMeta{
			chunkID:   id,
			slabClass: -1,
		}
		sa.freeChunks = append(sa.freeChunks, id)
	}

	return sa, nil
}

// ChunkFilePath returns the filesystem path for the given chunk ID.
func (s *SlabAllocator) ChunkFilePath(chunkID uint32) string {
	return filepath.Join(s.cacheDir, "chunks", fmt.Sprintf("chunk_%05d.dat", chunkID))
}

// SlabOffset returns the byte offset of slabIndex within a chunk of the given class.
func (s *SlabAllocator) SlabOffset(slabClass int, slabIndex int) int64 {
	return int64(slabIndex) * int64(SlabClasses[slabClass].SlabSize)
}

// Allocate finds (or assigns) a chunk for the requested slab class and returns
// a free slab within it. It is safe for concurrent use.
func (s *SlabAllocator) Allocate(slabClass int) (chunkID uint32, slabIndex int, err error) {
	if slabClass < 0 || slabClass >= len(SlabClasses) {
		return 0, 0, fmt.Errorf("pagecache: invalid slab class %d", slabClass)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Try to use an existing partial chunk for this class.
	for len(s.partialChunks[slabClass]) > 0 {
		cid := s.partialChunks[slabClass][len(s.partialChunks[slabClass])-1]
		idx := s.allocFromChunk(&s.chunks[cid])
		if idx >= 0 {
			// If the chunk is now full, remove it from the partial list.
			if s.chunks[cid].usedSlabs == SlabClasses[slabClass].SlabsPerChunk {
				s.partialChunks[slabClass] = s.partialChunks[slabClass][:len(s.partialChunks[slabClass])-1]
			}
			return cid, idx, nil
		}
		// Bitmap said free but we couldn't find one — shouldn't happen; drop it.
		s.partialChunks[slabClass] = s.partialChunks[slabClass][:len(s.partialChunks[slabClass])-1]
	}

	// No partial chunk available; grab one from the free pool.
	if len(s.freeChunks) == 0 {
		return 0, 0, fmt.Errorf("pagecache: out of chunks for slab class %d", slabClass)
	}
	cid := s.freeChunks[len(s.freeChunks)-1]
	s.freeChunks = s.freeChunks[:len(s.freeChunks)-1]

	s.assignChunk(cid, slabClass)

	idx := s.allocFromChunk(&s.chunks[cid])
	if idx < 0 {
		// Should never happen on a freshly assigned chunk.
		return 0, 0, fmt.Errorf("pagecache: internal error: no free slab in fresh chunk %d", cid)
	}

	// If there are still free slabs, add to partials.
	if s.chunks[cid].usedSlabs < SlabClasses[slabClass].SlabsPerChunk {
		s.partialChunks[slabClass] = append(s.partialChunks[slabClass], cid)
	}
	return cid, idx, nil
}

// Free releases a previously allocated slab. If the chunk becomes completely
// empty it is returned to the free pool so it can be reused by any class.
func (s *SlabAllocator) Free(chunkID uint32, slabIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cm := &s.chunks[chunkID]
	class := cm.slabClass
	if class < 0 {
		return // already unassigned — nothing to do
	}

	word := slabIndex / 64
	bit := uint(slabIndex % 64)
	if cm.freeBitmap[word]&(1<<bit) != 0 {
		return // double free — ignore
	}

	wasFull := cm.usedSlabs == SlabClasses[class].SlabsPerChunk

	cm.freeBitmap[word] |= 1 << bit
	cm.usedSlabs--

	if cm.usedSlabs == 0 {
		// Chunk is completely empty — unassign and return to free pool.
		s.removeFromPartials(class, chunkID)
		cm.slabClass = -1
		cm.freeBitmap = nil
		s.freeChunks = append(s.freeChunks, chunkID)
		return
	}

	if wasFull {
		// Was full, now has space — add back to partials.
		s.partialChunks[class] = append(s.partialChunks[class], chunkID)
	}
}

// ---- internal helpers (caller must hold s.mu) ----

// assignChunk sets up a free chunk for a specific slab class, initialising its
// bitmap with all slabs marked free.
func (s *SlabAllocator) assignChunk(chunkID uint32, slabClass int) {
	sc := SlabClasses[slabClass]
	nWords := (sc.SlabsPerChunk + 63) / 64
	bm := make([]uint64, nWords)

	// Set all valid slab bits to 1 (free).
	remaining := sc.SlabsPerChunk
	for i := range bm {
		if remaining >= 64 {
			bm[i] = ^uint64(0)
			remaining -= 64
		} else {
			bm[i] = (1 << uint(remaining)) - 1
			remaining = 0
		}
	}

	s.chunks[chunkID] = chunkMeta{
		chunkID:    chunkID,
		slabClass:  slabClass,
		usedSlabs:  0,
		freeBitmap: bm,
	}
}

// allocFromChunk finds the first free slab in cm, marks it used, and returns
// its index. Returns -1 if no free slab exists.
func (s *SlabAllocator) allocFromChunk(cm *chunkMeta) int {
	for i, word := range cm.freeBitmap {
		if word == 0 {
			continue
		}
		bit := bits.TrailingZeros64(word)
		cm.freeBitmap[i] &^= 1 << uint(bit)
		cm.usedSlabs++
		return i*64 + bit
	}
	return -1
}

// removeFromPartials removes chunkID from the partial list of the given class.
func (s *SlabAllocator) removeFromPartials(class int, chunkID uint32) {
	list := s.partialChunks[class]
	for i, id := range list {
		if id == chunkID {
			s.partialChunks[class] = append(list[:i], list[i+1:]...)
			return
		}
	}
}
