package pagecache

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reyoung/afs/pkg/pagecachepb"
)

// Service implements the PageCacheService gRPC server.
type Service struct {
	pagecachepb.UnimplementedPageCacheServiceServer

	slab     *SlabAllocator
	index    *PageIndex
	eviction *evictionPolicy
	leases   *LeaseManager

	mu       sync.Mutex
	putLocks sync.Map // pageKey → *sync.Mutex for concurrent PutPage dedup

	hitCount  atomic.Int64
	missCount atomic.Int64

	sessions   map[string]struct{}
	sessionsMu sync.Mutex
}

// NewService creates a new page cache service.
func NewService(cacheDir string, maxCacheBytes int64, leaseTimeout time.Duration) (*Service, error) {
	slab, err := NewSlabAllocator(cacheDir, maxCacheBytes)
	if err != nil {
		return nil, fmt.Errorf("init slab allocator: %w", err)
	}

	maxPages := int(maxCacheBytes / (64 << 10)) // rough estimate using 64K avg page
	if maxPages < 1024 {
		maxPages = 1024
	}

	index := NewPageIndex()
	eviction := newEvictionPolicy(maxPages)

	s := &Service{
		slab:     slab,
		index:    index,
		eviction: eviction,
		sessions: make(map[string]struct{}),
	}

	s.leases = NewLeaseManager(leaseTimeout, func(key pageKey) {
		s.index.mu.Lock()
		if entry, ok := s.index.entries[key]; ok {
			entry.PinCount--
		}
		s.index.mu.Unlock()
	})

	return s, nil
}

// StartBackgroundTasks starts lease cleanup and other periodic tasks.
func (s *Service) StartBackgroundTasks(ctx context.Context) {
	s.leases.StartCleanupLoop(ctx, 5*time.Second)
}

func (s *Service) PutPage(stream pagecachepb.PageCacheService_PutPageServer) error {
	var header *pagecachepb.PutPageHeader
	var data []byte

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if chunk.Header != nil && header == nil {
			header = chunk.Header
			data = make([]byte, 0, header.TotalSize)
		}
		if len(chunk.Data) > 0 {
			data = append(data, chunk.Data...)
		}
	}

	if header == nil {
		return stream.SendAndClose(&pagecachepb.PutPageResponse{
			Status: pagecachepb.PutPageStatus_PUT_PAGE_INVALID,
		})
	}

	if uint32(len(data)) != header.TotalSize {
		return stream.SendAndClose(&pagecachepb.PutPageResponse{
			Status: pagecachepb.PutPageStatus_PUT_PAGE_INVALID,
		})
	}

	key := MakePageKey(header.Digest, header.PageId)

	// Dedup: check if already cached
	if _, ok := s.index.Get(key); ok {
		return stream.SendAndClose(&pagecachepb.PutPageResponse{
			Status: pagecachepb.PutPageStatus_PUT_PAGE_ALREADY_EXISTS,
		})
	}

	// Per-key mutex for concurrent PutPage dedup
	lockI, _ := s.putLocks.LoadOrStore(key, &sync.Mutex{})
	lock := lockI.(*sync.Mutex)
	lock.Lock()
	defer func() {
		lock.Unlock()
		s.putLocks.Delete(key)
	}()

	// Double-check after acquiring lock
	if _, ok := s.index.Get(key); ok {
		return stream.SendAndClose(&pagecachepb.PutPageResponse{
			Status: pagecachepb.PutPageStatus_PUT_PAGE_ALREADY_EXISTS,
		})
	}

	// Determine slab class
	slabClass := SlabClassForSize(len(data))

	// Try to allocate, evict if needed
	chunkID, slabIndex, err := s.allocateWithEviction(slabClass)
	if err != nil {
		return stream.SendAndClose(&pagecachepb.PutPageResponse{
			Status: pagecachepb.PutPageStatus_PUT_PAGE_NO_SPACE,
		})
	}

	// Write data to disk
	chunkPath := s.slab.ChunkFilePath(chunkID)
	offset := s.slab.SlabOffset(slabClass, slabIndex)

	f, err := os.OpenFile(chunkPath, os.O_WRONLY, 0o644)
	if err != nil {
		s.slab.Free(chunkID, slabIndex)
		return fmt.Errorf("open chunk file: %w", err)
	}
	_, err = f.WriteAt(data, offset)
	f.Close()
	if err != nil {
		s.slab.Free(chunkID, slabIndex)
		return fmt.Errorf("write chunk data: %w", err)
	}

	// Update index and eviction
	entry := &CacheEntry{
		ChunkID:   chunkID,
		SlabIndex: slabIndex,
		SlabClass: slabClass,
		DataSize:  uint32(len(data)),
	}
	s.index.Put(key, entry)
	s.eviction.Add(key)

	return stream.SendAndClose(&pagecachepb.PutPageResponse{
		Status: pagecachepb.PutPageStatus_PUT_PAGE_OK,
	})
}

func (s *Service) GetPage(ctx context.Context, req *pagecachepb.GetPageRequest) (*pagecachepb.GetPageResponse, error) {
	key := MakePageKey(req.Digest, req.PageId)

	entry, ok := s.index.Get(key)
	if !ok {
		s.missCount.Add(1)
		return &pagecachepb.GetPageResponse{
			Status: pagecachepb.GetPageStatus_CACHE_MISS,
		}, nil
	}

	s.hitCount.Add(1)

	// Pin the entry
	s.index.mu.Lock()
	entry.PinCount++
	s.index.mu.Unlock()

	// Record access for eviction
	s.eviction.Access(key)

	// Grant lease
	leaseID := s.leases.Grant("", key)

	chunkPath := s.slab.ChunkFilePath(entry.ChunkID)
	offset := s.slab.SlabOffset(entry.SlabClass, entry.SlabIndex)

	return &pagecachepb.GetPageResponse{
		Status:        pagecachepb.GetPageStatus_CACHE_HIT,
		CacheFilePath: chunkPath,
		CacheOffset:   uint64(offset),
		DataSize:      entry.DataSize,
		LeaseId:       leaseID,
	}, nil
}

func (s *Service) Release(ctx context.Context, req *pagecachepb.ReleaseRequest) (*pagecachepb.ReleaseResponse, error) {
	_ = s.leases.Release(req.LeaseId)
	return &pagecachepb.ReleaseResponse{}, nil
}

func (s *Service) BatchGetPage(ctx context.Context, req *pagecachepb.BatchGetPageRequest) (*pagecachepb.BatchGetPageResponse, error) {
	resp := &pagecachepb.BatchGetPageResponse{
		Pages: make([]*pagecachepb.GetPageResponse, len(req.Pages)),
	}
	for i, page := range req.Pages {
		r, err := s.GetPage(ctx, page)
		if err != nil {
			return nil, err
		}
		resp.Pages[i] = r
	}
	return resp, nil
}

func (s *Service) BatchRelease(ctx context.Context, req *pagecachepb.BatchReleaseRequest) (*pagecachepb.BatchReleaseResponse, error) {
	s.leases.BatchRelease(req.LeaseIds)
	return &pagecachepb.BatchReleaseResponse{}, nil
}

func (s *Service) Register(ctx context.Context, req *pagecachepb.RegisterRequest) (*pagecachepb.RegisterResponse, error) {
	sessionID := fmt.Sprintf("session-%s-%d", req.ClientId, time.Now().UnixNano())
	s.sessionsMu.Lock()
	s.sessions[sessionID] = struct{}{}
	s.sessionsMu.Unlock()
	log.Printf("page-cache: client registered session=%s client=%s", sessionID, req.ClientId)
	return &pagecachepb.RegisterResponse{SessionId: sessionID}, nil
}

func (s *Service) GetStats(ctx context.Context, req *pagecachepb.GetStatsRequest) (*pagecachepb.GetStatsResponse, error) {
	s.sessionsMu.Lock()
	connectedClients := uint32(len(s.sessions))
	s.sessionsMu.Unlock()

	return &pagecachepb.GetStatsResponse{
		TotalPages:       uint64(s.index.Len()),
		TotalBytes:       0, // TODO: track
		MaxBytes:         uint64(s.slab.maxChunks) * ChunkSize,
		HitCount:         uint64(s.hitCount.Load()),
		MissCount:        uint64(s.missCount.Load()),
		ActiveLeases:     uint32(s.leases.ActiveCount()),
		ConnectedClients: connectedClients,
	}, nil
}

// allocateWithEviction tries to allocate a slab, evicting if needed.
func (s *Service) allocateWithEviction(slabClass int) (uint32, int, error) {
	// Try direct allocation first
	chunkID, slabIndex, err := s.slab.Allocate(slabClass)
	if err == nil {
		return chunkID, slabIndex, nil
	}

	// Need to evict
	for attempts := 0; attempts < 64; attempts++ {
		victim := s.eviction.FindVictim(func(key pageKey) bool {
			entry, ok := s.index.Get(key)
			return ok && entry.PinCount > 0
		})
		if victim == nil {
			break
		}

		entry, ok := s.index.Get(*victim)
		if !ok {
			continue
		}

		// Evict: free slab, remove from index and eviction
		s.slab.Free(entry.ChunkID, entry.SlabIndex)
		s.index.Delete(*victim)
		s.eviction.Remove(*victim)

		// Try to allocate again
		chunkID, slabIndex, err = s.slab.Allocate(slabClass)
		if err == nil {
			return chunkID, slabIndex, nil
		}
	}

	return 0, 0, fmt.Errorf("no space after eviction attempts")
}
