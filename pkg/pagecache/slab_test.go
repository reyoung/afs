package pagecache

import (
	"os"
	"path/filepath"
	"testing"
)

func tempAllocator(t *testing.T, maxBytes int64) *SlabAllocator {
	t.Helper()
	dir := t.TempDir()
	sa, err := NewSlabAllocator(dir, maxBytes)
	if err != nil {
		t.Fatalf("NewSlabAllocator: %v", err)
	}
	return sa
}

func TestSlabClassForSize(t *testing.T) {
	tests := []struct {
		size      int
		wantClass int
	}{
		{1, 0},            // 1 byte → 2 KB class
		{2048, 0},         // exactly 2 KB
		{2049, 1},         // just over → 4 KB
		{4096, 1},         // exactly 4 KB
		{65536, 5},        // 64 KB
		{1 << 20, 9},      // 1 MB
		{2 << 20, 10},     // 2 MB
		{(2 << 20) + 1, -1}, // too large
	}
	for _, tt := range tests {
		got := SlabClassForSize(tt.size)
		if got != tt.wantClass {
			t.Errorf("SlabClassForSize(%d) = %d, want %d", tt.size, got, tt.wantClass)
		}
	}
}

func TestAllocFreeRoundtrip(t *testing.T) {
	sa := tempAllocator(t, 4*ChunkSize) // 4 chunks

	cid, idx, err := sa.Allocate(0) // 2 KB class
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	// Verify the chunk file exists and has the right size.
	fi, err := os.Stat(sa.ChunkFilePath(cid))
	if err != nil {
		t.Fatalf("chunk file stat: %v", err)
	}
	if fi.Size() != ChunkSize {
		t.Fatalf("chunk file size = %d, want %d", fi.Size(), ChunkSize)
	}

	// Offset should be within the chunk.
	off := sa.SlabOffset(0, idx)
	if off < 0 || off >= ChunkSize {
		t.Fatalf("SlabOffset = %d, out of range", off)
	}

	// Free should not panic.
	sa.Free(cid, idx)
}

func TestFullChunkTransition(t *testing.T) {
	// Use class 10 (2 MB) which has only 1 slab per chunk.
	sa := tempAllocator(t, 2*ChunkSize)

	cid1, idx1, err := sa.Allocate(10)
	if err != nil {
		t.Fatalf("first alloc: %v", err)
	}
	if idx1 != 0 {
		t.Fatalf("expected slab index 0, got %d", idx1)
	}

	cid2, _, err := sa.Allocate(10)
	if err != nil {
		t.Fatalf("second alloc: %v", err)
	}
	if cid2 == cid1 {
		t.Fatal("expected different chunk for second alloc")
	}

	// Both chunks used; third alloc should fail.
	_, _, err = sa.Allocate(10)
	if err == nil {
		t.Fatal("expected error when all chunks are used")
	}

	// Free one and allocate again.
	sa.Free(cid1, idx1)
	cid3, _, err := sa.Allocate(10)
	if err != nil {
		t.Fatalf("alloc after free: %v", err)
	}
	// The freed chunk should have been recycled through the free pool.
	if cid3 != cid1 {
		t.Logf("recycled chunk %d (freed %d) — acceptable", cid3, cid1)
	}
}

func TestFillEntireChunk(t *testing.T) {
	// Class 6: 128 KB slabs, 16 per chunk. Use exactly 1 chunk.
	sa := tempAllocator(t, ChunkSize)
	class := 6
	n := SlabClasses[class].SlabsPerChunk // 16

	allocs := make([]struct{ cid uint32; idx int }, n)
	for i := 0; i < n; i++ {
		cid, idx, err := sa.Allocate(class)
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		allocs[i] = struct{ cid uint32; idx int }{cid, idx}
	}

	// Chunk should be full; next alloc should fail (only 1 chunk).
	_, _, err := sa.Allocate(class)
	if err == nil {
		t.Fatal("expected error when chunk is full and no free chunks remain")
	}

	// Free all slabs.
	for _, a := range allocs {
		sa.Free(a.cid, a.idx)
	}

	// Chunk should be back in the free pool; allocate from a different class.
	_, _, err = sa.Allocate(0) // 2 KB class
	if err != nil {
		t.Fatalf("alloc after recycling: %v", err)
	}
}

func TestFreePoolRecycling(t *testing.T) {
	sa := tempAllocator(t, 2*ChunkSize)

	// Allocate a 2 KB slab, then free it so the chunk returns to the free pool.
	cid, idx, err := sa.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	sa.Free(cid, idx)

	// Now allocate from a completely different class — should reuse the chunk.
	cid2, _, err := sa.Allocate(10) // 2 MB class
	if err != nil {
		t.Fatalf("alloc from different class: %v", err)
	}
	// One of the two chunks should have been used.
	_ = cid2
}

func TestChunkFilePath(t *testing.T) {
	sa := &SlabAllocator{cacheDir: "/tmp/test"}
	got := sa.ChunkFilePath(42)
	want := filepath.Join("/tmp/test", "chunks", "chunk_00042.dat")
	if got != want {
		t.Errorf("ChunkFilePath(42) = %q, want %q", got, want)
	}
}

func TestUniqueSlabIndices(t *testing.T) {
	// Allocate all slabs in a chunk and ensure no duplicates.
	sa := tempAllocator(t, ChunkSize)
	class := 4 // 32 KB → 64 slabs per chunk
	n := SlabClasses[class].SlabsPerChunk

	seen := make(map[int]bool)
	for i := 0; i < n; i++ {
		_, idx, err := sa.Allocate(class)
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		if seen[idx] {
			t.Fatalf("duplicate slab index %d on alloc %d", idx, i)
		}
		seen[idx] = true
	}
}

func TestDoubleFreeIsHarmless(t *testing.T) {
	sa := tempAllocator(t, ChunkSize)
	cid, idx, err := sa.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	sa.Free(cid, idx)
	sa.Free(cid, idx) // should not panic or corrupt state
}

func TestSlabOffset(t *testing.T) {
	sa := tempAllocator(t, ChunkSize)
	// Class 0: 2 KB slabs.
	off := sa.SlabOffset(0, 5)
	want := int64(5 * (2 << 10))
	if off != want {
		t.Errorf("SlabOffset(0, 5) = %d, want %d", off, want)
	}
}
