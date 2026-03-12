package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/registry"
)

func TestResolveImageAndFindImageProvider(t *testing.T) {
	t.Parallel()

	s := NewService()
	s.SetResolverFactory(func() Resolver {
		return &fakeResolver{
			layers: []registry.Layer{
				{Digest: "sha256:aaa", MediaType: "layer/a", Size: 111},
				{Digest: "sha256:bbb", MediaType: "layer/b", Size: 222},
			},
			runtimeConfig: registry.ImageRuntimeConfig{
				Entrypoint: []string{"/docker-entrypoint.sh"},
				Cmd:        []string{"nginx", "-g", "daemon off;"},
				Env:        []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "FOO=bar"},
				WorkingDir: "/work",
				User:       "1000:1001",
			},
		}
	})

	_, err := s.Heartbeat(context.Background(), &discoverypb.HeartbeatRequest{
		NodeId:       "node-a",
		Endpoint:     "10.0.0.1:50051",
		LayerDigests: []string{"sha256:aaa", "sha256:bbb"},
		LayerStats: []*discoverypb.LayerStat{
			{Digest: "sha256:aaa", AfsSize: 123},
			{Digest: "sha256:bbb", AfsSize: 456},
		},
		CacheMaxBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Heartbeat node-a: %v", err)
	}
	_, err = s.Heartbeat(context.Background(), &discoverypb.HeartbeatRequest{
		NodeId:       "node-b",
		Endpoint:     "10.0.0.2:50051",
		LayerDigests: []string{"sha256:aaa"},
	})
	if err != nil {
		t.Fatalf("Heartbeat node-b: %v", err)
	}

	resolved, err := s.ResolveImage(context.Background(), &discoverypb.ResolveImageRequest{
		Image:        "nginx",
		Tag:          "latest",
		PlatformOs:   "linux",
		PlatformArch: "amd64",
	})
	if err != nil {
		t.Fatalf("ResolveImage: %v", err)
	}
	if got := len(resolved.GetLayers()); got != 2 {
		t.Fatalf("resolved layers=%d, want 2", got)
	}
	if got := resolved.GetRuntimeConfig().GetEntrypoint(); len(got) != 1 || got[0] != "/docker-entrypoint.sh" {
		t.Fatalf("entrypoint=%v, want [/docker-entrypoint.sh]", got)
	}
	if got := resolved.GetRuntimeConfig().GetCmd(); len(got) != 3 || got[0] != "nginx" || got[1] != "-g" || got[2] != "daemon off;" {
		t.Fatalf("cmd=%v, want nginx default cmd", got)
	}
	if got := resolved.GetRuntimeConfig().GetWorkingDir(); got != "/work" {
		t.Fatalf("working_dir=%q, want /work", got)
	}
	if got := resolved.GetRuntimeConfig().GetUser(); got != "1000:1001" {
		t.Fatalf("user=%q, want 1000:1001", got)
	}
	if got := resolved.GetRuntimeConfig().GetEnv(); len(got) != 2 || got[1] != "FOO=bar" {
		t.Fatalf("env=%v, want image env", got)
	}

	resp, err := s.FindImageProvider(context.Background(), &discoverypb.FindImageProviderRequest{
		ImageKey: resolved.GetImageKey(),
	})
	if err != nil {
		t.Fatalf("FindImageProvider: %v", err)
	}
	if len(resp.GetServices()) != 1 {
		t.Fatalf("FindImageProvider services=%d, want 1", len(resp.GetServices()))
	}
	if got := resp.GetServices()[0].GetNodeId(); got != "node-a" {
		t.Fatalf("node_id=%q, want node-a", got)
	}
	if got := resp.GetServices()[0].GetCacheMaxBytes(); got != 1<<30 {
		t.Fatalf("cache_max_bytes=%d, want %d", got, 1<<30)
	}
	if got := len(resp.GetServices()[0].GetLayerStats()); got != 2 {
		t.Fatalf("layer_stats=%d, want 2", got)
	}
}

func TestFindImageCompatibilityUsesCachedImageIndex(t *testing.T) {
	t.Parallel()

	s := NewService()
	_, err := s.Heartbeat(context.Background(), &discoverypb.HeartbeatRequest{
		NodeId:        "node-a",
		Endpoint:      "10.0.0.1:50051",
		CachedImages:  []string{"nginx|latest|linux|amd64|"},
		CacheMaxBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	resp, err := s.FindImage(context.Background(), &discoverypb.FindImageRequest{
		ImageKey: "nginx|latest|linux|amd64|",
	})
	if err != nil {
		t.Fatalf("FindImage: %v", err)
	}
	if len(resp.GetServices()) != 1 {
		t.Fatalf("FindImage services=%d, want 1", len(resp.GetServices()))
	}
	if got := len(resp.GetServices()[0].GetCachedImages()); got != 1 {
		t.Fatalf("cached_images=%d, want 1", got)
	}
}

func TestFindProviderListsActiveServices(t *testing.T) {
	t.Parallel()

	s := NewService()
	s.ttl = 30 * time.Second
	base := time.Unix(1000, 0)
	s.now = func() time.Time { return base }

	_, err := s.Heartbeat(context.Background(), &discoverypb.HeartbeatRequest{
		NodeId:   "node-a",
		Endpoint: "10.0.0.1:50051",
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	s.now = func() time.Time { return base.Add(31 * time.Second) }
	s.pruneExpired()

	list, err := s.FindProvider(context.Background(), &discoverypb.FindProviderRequest{})
	if err != nil {
		t.Fatalf("FindProvider: %v", err)
	}
	if len(list.GetServices()) != 0 {
		t.Fatalf("services=%d, want 0 after ttl", len(list.GetServices()))
	}
}

func TestFindProviderByLayerDigest(t *testing.T) {
	t.Parallel()

	s := NewService()
	_, err := s.Heartbeat(context.Background(), &discoverypb.HeartbeatRequest{
		NodeId:       "node-a",
		Endpoint:     "10.0.0.1:50051",
		LayerDigests: []string{"sha256:aaa", "sha256:bbb"},
	})
	if err != nil {
		t.Fatalf("Heartbeat node-a: %v", err)
	}
	_, err = s.Heartbeat(context.Background(), &discoverypb.HeartbeatRequest{
		NodeId:       "node-b",
		Endpoint:     "10.0.0.2:50051",
		LayerDigests: []string{"sha256:ccc"},
	})
	if err != nil {
		t.Fatalf("Heartbeat node-b: %v", err)
	}

	resp, err := s.FindProvider(context.Background(), &discoverypb.FindProviderRequest{
		LayerDigest: "sha256:bbb",
	})
	if err != nil {
		t.Fatalf("FindProvider by layer digest: %v", err)
	}
	if got := len(resp.GetServices()); got != 1 {
		t.Fatalf("services=%d, want 1", got)
	}
	if got := resp.GetServices()[0].GetEndpoint(); got != "10.0.0.1:50051" {
		t.Fatalf("endpoint=%q, want 10.0.0.1:50051", got)
	}
}

func TestFindImageProviderRequiresImageKey(t *testing.T) {
	t.Parallel()

	s := NewService()
	_, err := s.FindImageProvider(context.Background(), &discoverypb.FindImageProviderRequest{})
	if err == nil {
		t.Fatal("expected error for empty image_key")
	}
}

type fakeResolver struct {
	layers        []registry.Layer
	runtimeConfig registry.ImageRuntimeConfig
}

func (f *fakeResolver) GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error) {
	_ = ctx
	_ = image
	_ = tag
	_ = os
	_ = arch
	_ = variant
	return append([]registry.Layer(nil), f.layers...), nil
}

func (f *fakeResolver) GetImageMetadataForPlatform(ctx context.Context, image, tag, os, arch, variant string) (registry.ImageMetadata, error) {
	_ = ctx
	_ = image
	_ = tag
	_ = os
	_ = arch
	_ = variant
	return registry.ImageMetadata{
		Layers: append([]registry.Layer(nil), f.layers...),
		RuntimeConfig: registry.ImageRuntimeConfig{
			Entrypoint: append([]string(nil), f.runtimeConfig.Entrypoint...),
			Cmd:        append([]string(nil), f.runtimeConfig.Cmd...),
			Env:        append([]string(nil), f.runtimeConfig.Env...),
			WorkingDir: f.runtimeConfig.WorkingDir,
			User:       f.runtimeConfig.User,
		},
	}, nil
}

func (f *fakeResolver) Login(registry, username, password string) error {
	_ = registry
	_ = username
	_ = password
	return nil
}

func (f *fakeResolver) LoginWithToken(registry, token string) error {
	_ = registry
	_ = token
	return nil
}
