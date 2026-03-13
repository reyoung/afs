package layerfuse

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
)

const benchmarkWarmFileSize = 8 << 20

func BenchmarkV2FileNodeReadWarmSequential64K(b *testing.B) {
	benchmarkV2FileNodeReadWarm(b, 64<<10, false)
}

func BenchmarkV2FileNodeReadWarmParallel64K(b *testing.B) {
	benchmarkV2FileNodeReadWarm(b, 64<<10, true)
}

func BenchmarkV2FileNodeReadWarmSequential128K(b *testing.B) {
	benchmarkV2FileNodeReadWarm(b, 128<<10, false)
}

func BenchmarkV2FileNodeReadWarmParallel128K(b *testing.B) {
	benchmarkV2FileNodeReadWarm(b, 128<<10, true)
}

func benchmarkV2FileNodeReadWarm(b *testing.B, chunkSize int, parallel bool) {
	b.Helper()
	reader := newV2ReaderWithSingleFile(b, "bench.bin", makeBenchmarkPayload(benchmarkWarmFileSize))
	section, err := reader.OpenFileSection("bench.bin")
	if err != nil {
		b.Fatalf("OpenFileSection() error = %v", err)
	}
	node := &FileNode{
		entry: layerformat.Entry{
			Path:             "bench.bin",
			Type:             layerformat.EntryTypeFile,
			Mode:             0o644,
			UncompressedSize: benchmarkWarmFileSize,
		},
		reader:        reader,
		directSection: &section,
	}

	b.ReportAllocs()
	b.SetBytes(int64(chunkSize))
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			dest := make([]byte, chunkSize)
			offset := int64(0)
			limit := section.Size - int64(chunkSize)
			if limit < 0 {
				limit = 0
			}
			for pb.Next() {
				res, errno := node.Read(context.Background(), nil, dest, offset)
				if errno != 0 {
					b.Fatalf("Read() errno = %d", errno)
				}
				if got := readResultLen(b, res, dest); got == 0 {
					b.Fatal("Read() returned empty payload")
				}
				offset += int64(chunkSize)
				if offset > limit {
					offset = 0
				}
			}
		})
		return
	}

	dest := make([]byte, chunkSize)
	offset := int64(0)
	limit := section.Size - int64(chunkSize)
	if limit < 0 {
		limit = 0
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, errno := node.Read(context.Background(), nil, dest, offset)
		if errno != 0 {
			b.Fatalf("Read() errno = %d", errno)
		}
		if got := readResultLen(b, res, dest); got == 0 {
			b.Fatal("Read() returned empty payload")
		}
		offset += int64(chunkSize)
		if offset > limit {
			offset = 0
		}
	}
}

func TestV2FileNodeReadAllowsConcurrentReadAt(t *testing.T) {
	t.Parallel()

	tracker := &trackingReaderAt{
		size:  1 << 20,
		hold:  make(chan struct{}),
		ready: make(chan struct{}, 16),
	}
	section := layerformat.FileSection{
		RA:     tracker,
		Offset: 0,
		Size:   tracker.size,
	}
	node := &FileNode{
		entry: layerformat.Entry{
			Path:             "bench.bin",
			Type:             layerformat.EntryTypeFile,
			Mode:             0o644,
			UncompressedSize: tracker.size,
		},
		directSection: &section,
	}

	const workers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			dest := make([]byte, 64<<10)
			res, errno := node.Read(context.Background(), nil, dest, int64(i*4096))
			if errno != 0 {
				errCh <- io.ErrUnexpectedEOF
				return
			}
			if got := readResultLen(t, res, dest); got == 0 {
				errCh <- io.ErrNoProgress
			}
		}()
	}

	for i := 0; i < workers; i++ {
		<-tracker.ready
	}
	if tracker.maxInflight.Load() < 2 {
		t.Fatalf("maxInflight=%d, want >= 2", tracker.maxInflight.Load())
	}
	close(tracker.hold)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent read failed: %v", err)
		}
	}
}

type trackingReaderAt struct {
	size int64

	hold  chan struct{}
	ready chan struct{}

	inflight    atomic.Int64
	maxInflight atomic.Int64
}

func (r *trackingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	current := r.inflight.Add(1)
	updateMax(&r.maxInflight, current)
	r.ready <- struct{}{}
	<-r.hold
	defer r.inflight.Add(-1)

	if off >= r.size {
		return 0, io.EOF
	}
	remain := r.size - off
	if int64(len(p)) > remain {
		p = p[:remain]
	}
	for i := range p {
		p[i] = byte((int(off) + i) & 0xff)
	}
	if int64(len(p)) < remain {
		return len(p), nil
	}
	return len(p), io.EOF
}

func updateMax(dst *atomic.Int64, current int64) {
	for {
		old := dst.Load()
		if current <= old {
			return
		}
		if dst.CompareAndSwap(old, current) {
			return
		}
	}
}

func readResultLen(tb testing.TB, res fuse.ReadResult, buf []byte) int {
	tb.Helper()
	defer res.Done()
	data, status := res.Bytes(buf)
	if !status.Ok() {
		tb.Fatalf("ReadResult status = %v", status)
	}
	return len(data)
}

func makeBenchmarkPayload(n int) []byte {
	payload := make([]byte, n)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}
