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
		Image: "busybox",
		Tag:   "latest",
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
	if string(readResp.GetData()) != "AFSLYR01" {
		t.Fatalf("ReadLayer data=%q, want AFSLYR01", string(readResp.GetData()))
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
	svc.SetCacheLimitBytes(3500)

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
	svc.SetCacheLimitBytes(100)
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

type fakeFetcher struct {
	layers         []registry.Layer
	byDigest       map[string][]byte
	downloadCalls  int
	getLayersCalls int
}

func (f *fakeFetcher) GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error) {
	_ = ctx
	_ = image
	_ = tag
	_ = os
	_ = arch
	_ = variant
	f.getLayersCalls++
	return append([]registry.Layer(nil), f.layers...), nil
}

func (f *fakeFetcher) DownloadLayer(ctx context.Context, image, tag, digest string) (io.ReadCloser, error) {
	_ = ctx
	_ = image
	_ = tag
	f.downloadCalls++
	return io.NopCloser(bytes.NewReader(f.byDigest[digest])), nil
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
