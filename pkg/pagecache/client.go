package pagecache

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/pagecachepb"
)

const (
	defaultPageSize    = 64 << 10 // 64KB
	putPageChunkSize   = 64 << 10 // 64KB streaming chunks
	defaultDialTimeout = 3 * time.Second
)

// Client is a page cache client that communicates with the cache daemon via gRPC over UDS.
type Client struct {
	conn   *grpc.ClientConn
	stub   pagecachepb.PageCacheServiceClient
	closed bool
	mu     sync.Mutex
}

// NewClient creates a page cache client connected to the given UDS path.
func NewClient(udsPath string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	target := "unix://" + udsPath
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial page cache %s: %w", udsPath, err)
	}

	return &Client{
		conn: conn,
		stub: pagecachepb.NewPageCacheServiceClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// GetPage queries the cache for a page. Returns (response, nil) on success.
func (c *Client) GetPage(ctx context.Context, digest string, pageID uint64) (*pagecachepb.GetPageResponse, error) {
	return c.stub.GetPage(ctx, &pagecachepb.GetPageRequest{
		Digest: digest,
		PageId: pageID,
	})
}

// PutPage writes a page to the cache using client streaming.
func (c *Client) PutPage(ctx context.Context, digest string, pageID uint64, data []byte) (*pagecachepb.PutPageResponse, error) {
	stream, err := c.stub.PutPage(ctx)
	if err != nil {
		return nil, err
	}

	// Send first chunk with header
	first := &pagecachepb.PutPageChunk{
		Header: &pagecachepb.PutPageHeader{
			Digest:    digest,
			PageId:    pageID,
			TotalSize: uint32(len(data)),
		},
	}
	chunkEnd := putPageChunkSize
	if chunkEnd > len(data) {
		chunkEnd = len(data)
	}
	first.Data = data[:chunkEnd]
	if err := stream.Send(first); err != nil {
		return nil, err
	}

	// Send remaining chunks
	for offset := chunkEnd; offset < len(data); {
		end := offset + putPageChunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := &pagecachepb.PutPageChunk{
			Data: data[offset:end],
		}
		if err := stream.Send(chunk); err != nil {
			return nil, err
		}
		offset = end
	}

	return stream.CloseAndRecv()
}

// Release releases a lease.
func (c *Client) Release(ctx context.Context, leaseID uint64) error {
	_, err := c.stub.Release(ctx, &pagecachepb.ReleaseRequest{LeaseId: leaseID})
	return err
}

// BatchRelease releases multiple leases.
func (c *Client) BatchRelease(ctx context.Context, leaseIDs []uint64) error {
	_, err := c.stub.BatchRelease(ctx, &pagecachepb.BatchReleaseRequest{LeaseIds: leaseIDs})
	return err
}

// GetStats queries cache statistics.
func (c *Client) GetStats(ctx context.Context) (*pagecachepb.GetStatsResponse, error) {
	return c.stub.GetStats(ctx, &pagecachepb.GetStatsRequest{})
}

// CachedReaderAt wraps an io.ReaderAt and intercepts reads via the page cache.
// On cache hit, it pread's directly from the cache disk file (zero-copy).
// On cache miss, it reads from the underlying reader, writes to cache, and returns.
type CachedReaderAt struct {
	underlying io.ReaderAt
	client     *Client
	digest     string   // content digest from AFSLYR02 TOC
	fileSize   int64    // total file size
	pageSize   int      // page size in bytes (chosen based on file size)
}

// NewCachedReaderAt creates a CachedReaderAt for a file entry.
func NewCachedReaderAt(underlying io.ReaderAt, client *Client, digest string, fileSize int64) *CachedReaderAt {
	pageSize := choosePageSize(fileSize)
	return &CachedReaderAt{
		underlying: underlying,
		client:     client,
		digest:     digest,
		fileSize:   fileSize,
		pageSize:   pageSize,
	}
}

// choosePageSize picks the page size based on file size.
// Small files use smaller pages to reduce waste; large files use 64KB.
func choosePageSize(fileSize int64) int {
	if fileSize <= 2<<10 {
		return 2 << 10 // 2KB
	}
	if fileSize <= 4<<10 {
		return 4 << 10
	}
	if fileSize <= 8<<10 {
		return 8 << 10
	}
	if fileSize <= 16<<10 {
		return 16 << 10
	}
	if fileSize <= 32<<10 {
		return 32 << 10
	}
	return defaultPageSize // 64KB
}

// ReadAt implements io.ReaderAt with page cache interception.
func (r *CachedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.fileSize {
		return 0, io.EOF
	}

	totalRead := 0
	for totalRead < len(p) && off < r.fileSize {
		pageID := uint64(off / int64(r.pageSize))
		pageOff := int(off % int64(r.pageSize))

		// How much of this page do we need?
		pageRemain := r.pageSize - pageOff
		wantFromPage := len(p) - totalRead
		if wantFromPage > pageRemain {
			wantFromPage = pageRemain
		}
		// Don't read past file end
		if off+int64(wantFromPage) > r.fileSize {
			wantFromPage = int(r.fileSize - off)
		}

		n, err := r.readPageSlice(p[totalRead:totalRead+wantFromPage], pageID, pageOff)
		totalRead += n
		off += int64(n)
		if err != nil {
			if totalRead > 0 {
				return totalRead, nil
			}
			return totalRead, err
		}
	}

	if totalRead == 0 && off >= r.fileSize {
		return 0, io.EOF
	}
	return totalRead, nil
}

// readPageSlice reads a slice from a page, using cache when possible.
func (r *CachedReaderAt) readPageSlice(dst []byte, pageID uint64, pageOffset int) (int, error) {
	ctx := context.Background()

	// Try cache
	resp, err := r.client.GetPage(ctx, r.digest, pageID)
	if err == nil && resp.Status == pagecachepb.GetPageStatus_CACHE_HIT {
		// Zero-copy read from cache disk file
		n, readErr := r.preadCache(dst, resp.CacheFilePath, int64(resp.CacheOffset)+int64(pageOffset))
		// Release lease
		_ = r.client.Release(ctx, resp.LeaseId)
		if readErr == nil {
			return n, nil
		}
		// Cache read failed, fall through to remote
	}

	// Cache miss or error: read from underlying
	return r.readFromUnderlying(dst, pageID, pageOffset)
}

// preadCache reads directly from the cache chunk file.
func (r *CachedReaderAt) preadCache(dst []byte, path string, offset int64) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.ReadAt(dst, offset)
}

// readFromUnderlying reads a page from the remote source, caches it, and returns the slice.
func (r *CachedReaderAt) readFromUnderlying(dst []byte, pageID uint64, pageOffset int) (int, error) {
	// Read the full page from underlying
	pageStart := int64(pageID) * int64(r.pageSize)
	pageEnd := pageStart + int64(r.pageSize)
	if pageEnd > r.fileSize {
		pageEnd = r.fileSize
	}
	actualPageSize := int(pageEnd - pageStart)

	pageBuf := make([]byte, actualPageSize)
	n, err := r.underlying.ReadAt(pageBuf, pageStart)
	if err != nil && err != io.EOF {
		return 0, err
	}
	pageBuf = pageBuf[:n]

	// Async put to cache (best effort)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = r.client.PutPage(ctx, r.digest, pageID, pageBuf)
	}()

	// Copy requested portion to dst
	if pageOffset >= len(pageBuf) {
		return 0, io.EOF
	}
	copied := copy(dst, pageBuf[pageOffset:])
	return copied, nil
}
