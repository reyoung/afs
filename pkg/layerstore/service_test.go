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

func TestEnsureLayersCachesByDigest(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	layerBytes := buildTarGzip(t, map[string]string{"hello.txt": "hello from layer"})
	fake := &fakeFetcher{
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	first, err := svc.EnsureLayers(context.Background(), ensureRequest(layerDigest, layerformat.OCILayerTarGzipMediaType, int64(len(layerBytes))))
	if err != nil {
		t.Fatalf("first EnsureLayers: %v", err)
	}
	if got := len(first.GetLayers()); got != 1 {
		t.Fatalf("layers=%d, want 1", got)
	}
	if first.GetLayers()[0].GetCached() {
		t.Fatalf("first ensure should not be cached")
	}

	second, err := svc.EnsureLayers(context.Background(), ensureRequest(layerDigest, layerformat.OCILayerTarGzipMediaType, int64(len(layerBytes))))
	if err != nil {
		t.Fatalf("second EnsureLayers: %v", err)
	}
	if !second.GetLayers()[0].GetCached() {
		t.Fatalf("second ensure should hit cache")
	}
	if fake.downloadCalls != 1 {
		t.Fatalf("download calls=%d, want 1", fake.downloadCalls)
	}
}

func TestEnsureLayersSupportsDockerLayerMediaType(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	layerBytes := buildTarGzip(t, map[string]string{"docker.txt": "docker layer media type"})
	fake := &fakeFetcher{
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	resp, err := svc.EnsureLayers(context.Background(), ensureRequest(layerDigest, layerformat.DockerLayerTarGzipMediaType, int64(len(layerBytes))))
	if err != nil {
		t.Fatalf("EnsureLayers: %v", err)
	}
	if got := len(resp.GetLayers()); got != 1 {
		t.Fatalf("layers=%d, want 1", got)
	}
	layer := resp.GetLayers()[0]
	if layer.GetCached() {
		t.Fatalf("first ensure should not be cached")
	}
	if strings.TrimSpace(layer.GetCachePath()) == "" {
		t.Fatalf("cache path should not be empty")
	}

	data, err := os.ReadFile(layer.GetCachePath())
	if err != nil {
		t.Fatalf("read converted layer: %v", err)
	}
	if len(data) < 8 || string(data[:8]) != "AFSLYR01" {
		t.Fatalf("converted layer missing AFSLYR01 magic")
	}
}

func TestReadLayerReturnsMagicBytes(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	layerDigest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	layerBytes := buildTarGzip(t, map[string]string{"x": "y"})
	fake := &fakeFetcher{
		byDigest: map[string][]byte{layerDigest: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	if _, err := svc.EnsureLayers(context.Background(), ensureRequest(layerDigest, layerformat.OCILayerTarGzipMediaType, int64(len(layerBytes)))); err != nil {
		t.Fatalf("EnsureLayers: %v", err)
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

func TestPruneCacheEvictsOldestLayers(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	svc, err := NewService(tmp, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	oldDigest := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	newDigest := "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
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

func TestEnsureLayersRespectsCacheLimit(t *testing.T) {
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
		byDigest: map[string][]byte{
			layerA: layerBytesA,
			layerB: layerBytesB,
		},
	}
	svc.newFetcher = func() fetcher { return fake }

	if _, err := svc.EnsureLayers(context.Background(), &layerstorepb.EnsureLayersRequest{
		Source: &layerstorepb.LayerSource{Registry: "registry-1.docker.io", Repository: "library/busybox"},
		Layers: []*layerstorepb.LayerSpec{
			{Digest: layerA, MediaType: layerformat.OCILayerTarGzipMediaType, CompressedSize: int64(len(layerBytesA))},
			{Digest: layerB, MediaType: layerformat.OCILayerTarGzipMediaType, CompressedSize: int64(len(layerBytesB))},
		},
	}); err != nil {
		t.Fatalf("EnsureLayers: %v", err)
	}
	usage, err := svc.cacheUsageBytes()
	if err != nil {
		t.Fatalf("cacheUsageBytes: %v", err)
	}
	if usage > svc.CacheLimitBytes() {
		t.Fatalf("cache usage=%d exceeds limit=%d", usage, svc.CacheLimitBytes())
	}
}

func TestEnsureLayersFailsWhenLayersExceedCacheLimit(t *testing.T) {
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
		byDigest: map[string][]byte{layer: layerBytes},
	}
	svc.newFetcher = func() fetcher { return fake }

	_, err = svc.EnsureLayers(context.Background(), ensureRequest(layer, layerformat.OCILayerTarGzipMediaType, 200))
	if err == nil {
		t.Fatalf("expected EnsureLayers to fail for oversized request")
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
	byDigest      map[string][]byte
	downloadCalls int
}

func (f *fakeFetcher) DownloadLayerFromRepository(ctx context.Context, registryHost, repository, digest string) (io.ReadCloser, error) {
	_ = ctx
	_ = registryHost
	_ = repository
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

func ensureRequest(digest, mediaType string, compressedSize int64) *layerstorepb.EnsureLayersRequest {
	return &layerstorepb.EnsureLayersRequest{
		Source: &layerstorepb.LayerSource{
			Registry:   "registry-1.docker.io",
			Repository: "library/busybox",
		},
		Layers: []*layerstorepb.LayerSpec{{
			Digest:         digest,
			MediaType:      mediaType,
			CompressedSize: compressedSize,
		}},
	}
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
