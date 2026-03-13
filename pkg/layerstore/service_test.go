package layerstore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerstorepb"
	"github.com/reyoung/afs/pkg/registry"
)

func TestPullImageSkipsLocalFetchWhenPeerHasImage(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:abababababababababababababababababababababababababababababababab"
	layerBytes := buildTarGzip(t, map[string]string{"peer.txt": "from peer"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }
	svc.imagePeerChecker = func(ctx context.Context, imageKey string) (bool, error) {
		_ = ctx
		_ = imageKey
		return true, nil
	}

	resp, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"})
	if err != nil {
		t.Fatalf("PullImage: %v", err)
	}
	if got := len(resp.GetLayers()); got != 1 {
		t.Fatalf("layers=%d, want 1", got)
	}
	if resp.GetLayers()[0].GetCached() {
		t.Fatalf("layer should remain uncached locally when peer has image")
	}
	if got := resp.GetLayers()[0].GetAfsSize(); got != 0 {
		t.Fatalf("afs_size=%d, want 0 when local fetch is skipped", got)
	}
	if got := resp.GetLayers()[0].GetCachePath(); got != "" {
		t.Fatalf("cache_path=%q, want empty when local fetch is skipped", got)
	}
	if fake.downloadCalls != 0 {
		t.Fatalf("download calls=%d, want 0 when peer has image", fake.downloadCalls)
	}
}

func TestPullImageForceLocalFetchSkipsPeerImageCheck(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
	layerBytes := buildTarGzip(t, map[string]string{"force.txt": "from registry"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	peerCalls := 0
	svc.imagePeerChecker = func(ctx context.Context, imageKey string) (bool, error) {
		_ = ctx
		_ = imageKey
		peerCalls++
		return true, nil
	}

	if _, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{
		Image:           "busybox",
		Tag:             "latest",
		ForceLocalFetch: true,
	}); err != nil {
		t.Fatalf("PullImage(force): %v", err)
	}
	if peerCalls != 0 {
		t.Fatalf("peer fetch calls=%d, want 0 when force=true", peerCalls)
	}
	if fake.downloadCalls != 1 {
		t.Fatalf("download calls=%d, want 1 when force=true", fake.downloadCalls)
	}
}

func TestPullImageCachesByDigest(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	layerBytes := buildTarGzip(t, map[string]string{"hello.txt": "hello from layer"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	req := &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"}
	first, err := svc.PullImage(context.Background(), req)
	if err != nil {
		t.Fatalf("first PullImage: %v", err)
	}
	if got := len(first.GetLayers()); got != 1 {
		t.Fatalf("first PullImage layers=%d, want 1", got)
	}
	if first.GetLayers()[0].GetCached() {
		t.Fatalf("first PullImage should not be cached")
	}

	second, err := svc.PullImage(context.Background(), req)
	if err != nil {
		t.Fatalf("second PullImage: %v", err)
	}
	if !second.GetLayers()[0].GetCached() {
		t.Fatalf("second PullImage should be cached")
	}

	if fake.downloadCalls != 1 {
		t.Fatalf("download calls=%d, want 1", fake.downloadCalls)
	}
	if fake.getLayersCalls != 1 {
		t.Fatalf("get layers calls=%d, want 1 due to metadata cache", fake.getLayersCalls)
	}
}

func TestPullImageReusesSharedFetcher(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:abab1111abab1111abab1111abab1111abab1111abab1111abab1111abab1111"
	layerBytes := buildTarGzip(t, map[string]string{"hello.txt": "hello from layer"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	fetcherCreations := 0
	svc.newFetcher = func() fetcher {
		fetcherCreations++
		return fake
	}

	req := &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"}
	if _, err := svc.PullImage(context.Background(), req); err != nil {
		t.Fatalf("first PullImage: %v", err)
	}
	if _, err := svc.PullImage(context.Background(), req); err != nil {
		t.Fatalf("second PullImage: %v", err)
	}
	if fetcherCreations != 1 {
		t.Fatalf("fetcher creations=%d, want 1", fetcherCreations)
	}
}

func TestPullImageSupportsDockerLayerMediaType(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	layerBytes := buildTarGzip(t, map[string]string{"docker.txt": "docker layer media type"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.DockerLayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	resp, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"})
	if err != nil {
		t.Fatalf("PullImage: %v", err)
	}
	if got := len(resp.GetLayers()); got != 1 {
		t.Fatalf("layers=%d, want 1", got)
	}
	layer := resp.GetLayers()[0]
	if layer.GetCached() {
		t.Fatalf("first PullImage should not be cached")
	}
	if strings.TrimSpace(layer.GetCachePath()) == "" {
		t.Fatalf("cache path should not be empty")
	}

	data, err := os.ReadFile(layer.GetCachePath())
	if err != nil {
		t.Fatalf("read converted layer: %v", err)
	}
	if len(data) < 8 || string(data[:8]) != "AFSLYR02" {
		t.Fatalf("converted layer missing AFSLYR02 magic, got %q", string(data[:8]))
	}
}

func TestReadLayerReturnsMagicBytes(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	layerBytes := buildTarGzip(t, map[string]string{"x": "y"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	_, err = svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"})
	if err != nil {
		t.Fatalf("PullImage: %v", err)
	}

	readResp, err := svc.ReadLayer(context.Background(), &layerstorepb.ReadLayerRequest{
		Digest: layerDigest,
		Offset: 0,
		Length: 8,
	})
	if err != nil {
		t.Fatalf("ReadLayer: %v", err)
	}
	if string(readResp.GetData()) != "AFSLYR02" {
		t.Fatalf("ReadLayer data=%q, want AFSLYR02", string(readResp.GetData()))
	}
}

func TestPullImageForceBypassesMetadataCache(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	layerBytes := buildTarGzip(t, map[string]string{"a.txt": "a"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	req := &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"}
	if _, err := svc.PullImage(context.Background(), req); err != nil {
		t.Fatalf("first PullImage: %v", err)
	}
	if _, err := svc.PullImage(context.Background(), req); err != nil {
		t.Fatalf("second PullImage: %v", err)
	}
	if _, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest", ForceLocalFetch: true}); err != nil {
		t.Fatalf("forced PullImage: %v", err)
	}

	if fake.getLayersCalls != 2 {
		t.Fatalf("get layers calls=%d, want 2 (initial + force refresh)", fake.getLayersCalls)
	}
}

func TestEnsureLayersDownloadsConcurrently(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerA := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
	layerB := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2"
	layerBytesA := buildTarGzip(t, map[string]string{"a.txt": "layer-a"})
	layerBytesB := buildTarGzip(t, map[string]string{"b.txt": "layer-b"})

	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var mu sync.Mutex
	current := 0
	maxConcurrent := 0

	fake := &fakeFetcher{
		byDigest: map[string][]byte{
			layerA: layerBytesA,
			layerB: layerBytesB,
		},
		onDownload: func(digest string) {
			_ = digest
			mu.Lock()
			current++
			if current > maxConcurrent {
				maxConcurrent = current
			}
			mu.Unlock()
			started <- struct{}{}
			<-release
			mu.Lock()
			current--
			mu.Unlock()
		},
	}
	svc.newFetcher = func() fetcher { return fake }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := svc.EnsureLayers(ctx, &layerstorepb.EnsureLayersRequest{
			Image: "busybox",
			Tag:   "latest",
			Layers: []*layerstorepb.Layer{
				{Digest: layerA, MediaType: layerformat.OCILayerTarGzipMediaType, CompressedSize: int64(len(layerBytesA))},
				{Digest: layerB, MediaType: layerformat.OCILayerTarGzipMediaType, CompressedSize: int64(len(layerBytesB))},
			},
		})
		errCh <- err
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("download %d did not start before timeout", i+1)
		}
	}
	close(release)

	if err := <-errCh; err != nil {
		t.Fatalf("EnsureLayers: %v", err)
	}
	if maxConcurrent < 2 {
		t.Fatalf("maxConcurrent=%d, want at least 2", maxConcurrent)
	}
}

func TestHasImage(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	layerDigest := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	layerBytes := buildTarGzip(t, map[string]string{"f": "1"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	miss, err := svc.HasImage(context.Background(), &layerstorepb.HasImageRequest{Image: "busybox", Tag: "latest"})
	if err != nil {
		t.Fatalf("HasImage miss: %v", err)
	}
	if miss.GetFound() {
		t.Fatalf("expected miss before pull")
	}

	if _, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"}); err != nil {
		t.Fatalf("PullImage: %v", err)
	}

	hit, err := svc.HasImage(context.Background(), &layerstorepb.HasImageRequest{Image: "busybox", Tag: "latest"})
	if err != nil {
		t.Fatalf("HasImage hit: %v", err)
	}
	if !hit.GetFound() {
		t.Fatalf("expected hit after pull")
	}
}

func TestPruneCacheEvictsOldestLayers(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	oldDigest := "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	newDigest := "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	oldPath, err := svc.layerPath(oldDigest)
	if err != nil {
		t.Fatalf("old layerPath: %v", err)
	}
	newPath, err := svc.layerPath(newDigest)
	if err != nil {
		t.Fatalf("new layerPath: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o755); err != nil {
		t.Fatalf("mkdir old path: %v", err)
	}
	if err := os.WriteFile(oldPath, bytes.Repeat([]byte("a"), 1024), 0o644); err != nil {
		t.Fatalf("write old layer: %v", err)
	}
	if err := os.WriteFile(newPath, bytes.Repeat([]byte("b"), 1024), 0o644); err != nil {
		t.Fatalf("write new layer: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(oldPath, oldTime, oldTime); err != nil {
		t.Fatalf("set old mtime: %v", err)
	}

	resp, err := svc.PruneCache(context.Background(), &layerstorepb.PruneCacheRequest{Percent: 50})
	if err != nil {
		t.Fatalf("PruneCache: %v", err)
	}
	if resp.GetEvictedLayers() == 0 {
		t.Fatalf("expected at least one evicted layer")
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expected oldest layer to be evicted, stat err=%v", err)
	}
}

func TestPullImageRespectsCacheLimit(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.SetCacheLimitBytes(50000)

	layerA := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	layerB := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	layerBytesA := buildTarGzip(t, map[string]string{"a.txt": strings.Repeat("A", 4096)})
	layerBytesB := buildTarGzip(t, map[string]string{"b.txt": strings.Repeat("B", 4096)})
	fake := &fakeFetcher{
		layers: []registry.Layer{
			{Digest: layerA, MediaType: layerformat.OCILayerTarGzipMediaType, Size: int64(len(layerBytesA))},
			{Digest: layerB, MediaType: layerformat.OCILayerTarGzipMediaType, Size: int64(len(layerBytesB))},
		},
		byDigest: map[string][]byte{
			layerA: layerBytesA,
			layerB: layerBytesB,
		},
	}
	svc.newFetcher = func() fetcher { return fake }

	if _, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"}); err != nil {
		t.Fatalf("PullImage: %v", err)
	}
	usage, err := svc.cacheUsageBytes()
	if err != nil {
		t.Fatalf("cacheUsageBytes: %v", err)
	}
	if usage > svc.CacheLimitBytes() {
		t.Fatalf("cache usage=%d exceeds limit=%d", usage, svc.CacheLimitBytes())
	}
}

func TestPullImageFailsWhenImageExceedsCacheLimit(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.SetCacheLimitBytes(100)

	layer := "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	layerBytes := buildTarGzip(t, map[string]string{"c.txt": strings.Repeat("C", 1024)})
	fake := &fakeFetcher{
		layers: []registry.Layer{
			{Digest: layer, MediaType: layerformat.OCILayerTarGzipMediaType, Size: 200},
		},
		byDigest: map[string][]byte{layer: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	_, err = svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"})
	if err == nil {
		t.Fatalf("expected PullImage to fail for oversized image")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got err=%v", err)
	}
}

func TestReservePullSpaceConcurrentAccounting(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.SetCacheLimitBytes(500)
	layers := []registry.Layer{{Digest: "sha256:4444444444444444444444444444444444444444444444444444444444444444", Size: 80}}

	releaseA, err := svc.reservePullSpace(layers)
	if err != nil {
		t.Fatalf("first reservePullSpace: %v", err)
	}

	if _, err := svc.reservePullSpace(layers); err == nil {
		t.Fatalf("expected second reservation to fail while first reservation is held")
	}
	releaseA()

	releaseB, err := svc.reservePullSpace(layers)
	if err != nil {
		t.Fatalf("expected reservation to succeed after release: %v", err)
	}
	releaseB()
}

func TestDefaultFormatIsV2(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:5555555555555555555555555555555555555555555555555555555555555555"
	layerBytes := buildTarGzip(t, map[string]string{"default.txt": "default v2 content"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	resp, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"})
	if err != nil {
		t.Fatalf("PullImage: %v", err)
	}
	if len(resp.GetLayers()) != 1 {
		t.Fatalf("layers=%d, want 1", len(resp.GetLayers()))
	}

	cachePath := resp.GetLayers()[0].GetCachePath()
	if cachePath == "" {
		t.Fatal("cache path is empty")
	}
	// Verify it's stored with .v2.afslyr suffix
	if !strings.HasSuffix(cachePath, ".v2.afslyr") {
		t.Fatalf("cache path %q should end with .v2.afslyr", cachePath)
	}
	// Verify the file has AFSLYR02 magic
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	if len(data) < 8 || string(data[:8]) != "AFSLYR02" {
		t.Fatalf("expected AFSLYR02 magic, got %q", string(data[:8]))
	}
	// Verify the archive can be read and contains plain payload
	r, err := layerformat.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if r.FormatVersion() != layerformat.FormatV2 {
		t.Fatalf("expected FormatV2, got %d", r.FormatVersion())
	}
	content, err := r.ReadFile("default.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(content) != "default v2 content" {
		t.Fatalf("content=%q, want %q", string(content), "default v2 content")
	}
}

func TestExplicitV1Override(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.SetFormatVersion(layerformat.FormatV1)

	layerDigest := "sha256:6666666666666666666666666666666666666666666666666666666666666666"
	layerBytes := buildTarGzip(t, map[string]string{"v1.txt": "v1 explicit content"})
	fake := &fakeFetcher{
		layers: []registry.Layer{{
			Digest:    layerDigest,
			MediaType: layerformat.OCILayerTarGzipMediaType,
			Size:      int64(len(layerBytes)),
		}},
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	resp, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest"})
	if err != nil {
		t.Fatalf("PullImage: %v", err)
	}

	cachePath := resp.GetLayers()[0].GetCachePath()
	// V1 should use .afslyr (not .v2.afslyr)
	if strings.HasSuffix(cachePath, ".v2.afslyr") {
		t.Fatalf("V1 cache path %q should NOT end with .v2.afslyr", cachePath)
	}
	if !strings.HasSuffix(cachePath, ".afslyr") {
		t.Fatalf("V1 cache path %q should end with .afslyr", cachePath)
	}
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	if len(data) < 8 || string(data[:8]) != "AFSLYR01" {
		t.Fatalf("expected AFSLYR01 magic, got %q", string(data[:8]))
	}
}

func TestV2CacheEstimationUsesMultiplier(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	// Default is V2

	layers := []registry.Layer{
		{Digest: "sha256:7777777777777777777777777777777777777777777777777777777777777777", Size: 100},
	}
	required, _, err := svc.estimateRequiredBytesLocked(layers)
	if err != nil {
		t.Fatalf("estimateRequiredBytesLocked: %v", err)
	}
	// V2 should apply the multiplier (4x)
	expected := int64(100 * v2EstimateMultiplier)
	if required != expected {
		t.Fatalf("V2 estimate=%d, want %d (100 * %d)", required, expected, v2EstimateMultiplier)
	}

	// Switch to V1 and verify no multiplier
	svc.SetFormatVersion(layerformat.FormatV1)
	requiredV1, _, err := svc.estimateRequiredBytesLocked(layers)
	if err != nil {
		t.Fatalf("estimateRequiredBytesLocked V1: %v", err)
	}
	if requiredV1 != 100 {
		t.Fatalf("V1 estimate=%d, want 100", requiredV1)
	}
}

func TestV1V2CachePathsDoNotCollide(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	digest := "sha256:8888888888888888888888888888888888888888888888888888888888888888"

	// V2 path
	v2Path, err := layerPathForVersion(svc.layersDir, digest, layerformat.FormatV2)
	if err != nil {
		t.Fatalf("V2 layerPath: %v", err)
	}
	// V1 path
	v1Path, err := layerPathForVersion(svc.layersDir, digest, layerformat.FormatV1)
	if err != nil {
		t.Fatalf("V1 layerPath: %v", err)
	}

	if v1Path == v2Path {
		t.Fatalf("V1 and V2 paths should differ for same digest, both=%q", v1Path)
	}
	if !strings.HasSuffix(v2Path, ".v2.afslyr") {
		t.Fatalf("V2 path should end with .v2.afslyr: %q", v2Path)
	}
	if strings.HasSuffix(v1Path, ".v2.afslyr") {
		t.Fatalf("V1 path should NOT end with .v2.afslyr: %q", v1Path)
	}
}

type fakeFetcher struct {
	mu             sync.Mutex
	layers         []registry.Layer
	byDigest       map[string][]byte
	downloadCalls  int
	getLayersCalls int
	onDownload     func(digest string)
}

func (f *fakeFetcher) GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error) {
	_ = ctx
	_ = image
	_ = tag
	_ = os
	_ = arch
	_ = variant
	f.mu.Lock()
	f.getLayersCalls++
	layers := append([]registry.Layer(nil), f.layers...)
	f.mu.Unlock()
	return layers, nil
}

func (f *fakeFetcher) DownloadLayer(ctx context.Context, image, tag, digest string) (io.ReadCloser, error) {
	_ = ctx
	_ = image
	_ = tag
	f.mu.Lock()
	f.downloadCalls++
	payload := append([]byte(nil), f.byDigest[digest]...)
	onDownload := f.onDownload
	f.mu.Unlock()
	if onDownload != nil {
		onDownload(digest)
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (f *fakeFetcher) Login(registry, username, password string) error {
	_ = registry
	_ = username
	_ = password
	return nil
}

func (f *fakeFetcher) LoginWithToken(registry, token string) error {
	_ = registry
	_ = token
	return nil
}

func buildTarGzip(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for name, content := range files {
		b := []byte(content)
		hdr := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(b)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader(%s): %v", name, err)
		}
		if _, err := tw.Write(b); err != nil {
			t.Fatalf("Write(%s): %v", name, err)
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestMixedCacheScenarios(t *testing.T) {
	t.Parallel()

	t.Run("DefaultV2ServiceIgnoresV1Files", func(t *testing.T) {
		t.Parallel()
		tmp := t.TempDir()
		svc, err := NewService(tmp, nil)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		// Default is V2

		digest := "sha256:9999999999999999999999999999999999999999999999999999999999999999"

		// Create only V1 file in cache
		v1Path, err := layerPathForVersion(svc.layersDir, digest, layerformat.FormatV1)
		if err != nil {
			t.Fatalf("V1 layerPath: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(v1Path), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(v1Path, []byte("v1 content"), 0o644); err != nil {
			t.Fatalf("WriteFile V1: %v", err)
		}

		// V2 service should not find V1 file
		hasLayerResp, err := svc.HasLayer(context.Background(), &layerstorepb.HasLayerRequest{Digest: digest})
		if err != nil {
			t.Fatalf("HasLayer: %v", err)
		}
		if hasLayerResp.GetFound() {
			t.Fatalf("V2 service should not find V1 layer in cache")
		}
	})

	t.Run("ExplicitV1ServiceUsesV1Files", func(t *testing.T) {
		t.Parallel()
		tmp := t.TempDir()
		svc, err := NewService(tmp, nil)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		svc.SetFormatVersion(layerformat.FormatV1)

		digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

		// Create V1 file in cache
		v1Path, err := layerPathForVersion(svc.layersDir, digest, layerformat.FormatV1)
		if err != nil {
			t.Fatalf("V1 layerPath: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(v1Path), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(v1Path, []byte("v1 content"), 0o644); err != nil {
			t.Fatalf("WriteFile V1: %v", err)
		}

		// V1 service should find V1 file
		hasLayerResp, err := svc.HasLayer(context.Background(), &layerstorepb.HasLayerRequest{Digest: digest})
		if err != nil {
			t.Fatalf("HasLayer: %v", err)
		}
		if !hasLayerResp.GetFound() {
			t.Fatalf("V1 service should find V1 layer in cache")
		}
	})

	t.Run("V1V2CoexistWithoutConflict", func(t *testing.T) {
		t.Parallel()
		tmp := t.TempDir()

		digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

		// Create both V1 and V2 files for same digest
		v1Path, err := layerPathForVersion(filepath.Join(tmp, "layers"), digest, layerformat.FormatV1)
		if err != nil {
			t.Fatalf("V1 layerPath: %v", err)
		}
		v2Path, err := layerPathForVersion(filepath.Join(tmp, "layers"), digest, layerformat.FormatV2)
		if err != nil {
			t.Fatalf("V2 layerPath: %v", err)
		}

		if err := os.MkdirAll(filepath.Dir(v1Path), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(v1Path, []byte("v1 content"), 0o644); err != nil {
			t.Fatalf("WriteFile V1: %v", err)
		}
		if err := os.WriteFile(v2Path, []byte("v2 content"), 0o644); err != nil {
			t.Fatalf("WriteFile V2: %v", err)
		}

		// Test V1 service
		svcV1, err := NewService(tmp, nil)
		if err != nil {
			t.Fatalf("NewService V1: %v", err)
		}
		svcV1.SetFormatVersion(layerformat.FormatV1)
		hasLayerV1, err := svcV1.HasLayer(context.Background(), &layerstorepb.HasLayerRequest{Digest: digest})
		if err != nil {
			t.Fatalf("HasLayer V1: %v", err)
		}
		if !hasLayerV1.GetFound() {
			t.Fatalf("V1 service should find V1 layer")
		}

		// Test V2 service
		svcV2, err := NewService(tmp, nil)
		if err != nil {
			t.Fatalf("NewService V2: %v", err)
		}
		// Default is V2
		hasLayerV2, err := svcV2.HasLayer(context.Background(), &layerstorepb.HasLayerRequest{Digest: digest})
		if err != nil {
			t.Fatalf("HasLayer V2: %v", err)
		}
		if !hasLayerV2.GetFound() {
			t.Fatalf("V2 service should find V2 layer")
		}
	})
}

func TestHasLayer_V2ServiceDoesNotFindV1Only(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001"
	// Only place a V1 .afslyr file
	v1Path := filepath.Join(tmp, "layers", "sha256", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001.afslyr")
	if err := os.MkdirAll(filepath.Dir(v1Path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(v1Path, []byte("v1data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	// Default is V2
	resp, err := svc.HasLayer(context.Background(), &layerstorepb.HasLayerRequest{Digest: digest})
	if err != nil {
		t.Fatalf("HasLayer: %v", err)
	}
	if resp.GetFound() {
		t.Fatalf("V2 service should NOT find V1-only layer")
	}
}

func TestHasLayer_V1ServiceDoesNotFindV2Only(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0002"
	// Only place a V2 .v2.afslyr file
	v2Path := filepath.Join(tmp, "layers", "sha256", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0002.v2.afslyr")
	if err := os.MkdirAll(filepath.Dir(v2Path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(v2Path, []byte("v2data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.SetFormatVersion(layerformat.FormatV1)
	resp, err := svc.HasLayer(context.Background(), &layerstorepb.HasLayerRequest{Digest: digest})
	if err != nil {
		t.Fatalf("HasLayer: %v", err)
	}
	if resp.GetFound() {
		t.Fatalf("V1 service should NOT find V2-only layer")
	}
}

func TestStatLayer_V2ServiceDoesNotFindV1Only(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0003"
	v1Path := filepath.Join(tmp, "layers", "sha256", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0003.afslyr")
	if err := os.MkdirAll(filepath.Dir(v1Path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(v1Path, []byte("v1data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	_, statErr := svc.StatLayer(context.Background(), &layerstorepb.StatLayerRequest{Digest: digest})
	if statErr == nil {
		t.Fatalf("V2 StatLayer should fail for V1-only layer")
	}
	st, ok := status.FromError(statErr)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", statErr)
	}
}

func TestMetadataCacheKeyIncludesFormatVersion(t *testing.T) {
	t.Parallel()

	v1Key := metadataCacheKey("docker.io", "library/nginx", "latest", "linux", "amd64", "", layerformat.FormatV1)
	v2Key := metadataCacheKey("docker.io", "library/nginx", "latest", "linux", "amd64", "", layerformat.FormatV2)
	if v1Key == v2Key {
		t.Fatalf("V1 and V2 metadata cache keys should differ, both=%q", v1Key)
	}
}
