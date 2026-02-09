package layerstore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerstorepb"
	"github.com/reyoung/afs/pkg/registry"
)

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
	if _, err := svc.PullImage(context.Background(), &layerstorepb.PullImageRequest{Image: "busybox", Tag: "latest", Force: true}); err != nil {
		t.Fatalf("forced PullImage: %v", err)
	}

	if fake.getLayersCalls != 2 {
		t.Fatalf("get layers calls=%d, want 2 (initial + force refresh)", fake.getLayersCalls)
	}
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
