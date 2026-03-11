package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/registry"
)

func TestResolveImageAndFindCompleteProviders(t *testing.T) {
	t.Parallel()

	s := NewService()
	s.SetResolverFactory(func() Resolver {
		return &fakeResolver{
			layers: []registry.Layer{
				{Digest: "sha256:aaa", MediaType: "layer/a", Size: 111},
				{Digest: "sha256:bbb", MediaType: "layer/b", Size: 222},
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

	resp, err := s.FindImage(context.Background(), &discoverypb.FindImageRequest{
		ImageKey: resolved.GetImageKey(),
	})
	if err != nil {
		t.Fatalf("FindImage: %v", err)
	}
	if len(resp.GetServices()) != 1 {
		t.Fatalf("FindImage services=%d, want 1", len(resp.GetServices()))
	}
	if got := resp.GetServices()[0].GetNodeId(); got != "node-a" {
		t.Fatalf("node_id=%q, want node-a", got)
	}
	if got := resp.GetServices()[0].GetCacheMaxBytes(); got != 1<<30 {
		t.Fatalf("cache_max_bytes=%d, want %d", got, 1<<30)
	}
}

func TestPruneExpired(t *testing.T) {
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

	list, err := s.FindImage(context.Background(), &discoverypb.FindImageRequest{})
	if err != nil {
		t.Fatalf("FindImage: %v", err)
	}
	if len(list.GetServices()) != 0 {
		t.Fatalf("services=%d, want 0 after ttl", len(list.GetServices()))
	}
}

func TestFindImageByLayerDigest(t *testing.T) {
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

	resp, err := s.FindImage(context.Background(), &discoverypb.FindImageRequest{
		LayerDigest: "sha256:bbb",
	})
	if err != nil {
		t.Fatalf("FindImage by layer digest: %v", err)
	}
	if got := len(resp.GetServices()); got != 1 {
		t.Fatalf("services=%d, want 1", got)
	}
	if got := resp.GetServices()[0].GetEndpoint(); got != "10.0.0.1:50051" {
		t.Fatalf("endpoint=%q, want 10.0.0.1:50051", got)
	}
}

type fakeResolver struct {
	layers []registry.Layer
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
