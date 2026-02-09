package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/discoverypb"
)

func TestHeartbeatAndFindImage(t *testing.T) {
	t.Parallel()

	s := NewService()
	_, err := s.Heartbeat(context.Background(), &discoverypb.HeartbeatRequest{
		NodeId:       "node-a",
		Endpoint:     "10.0.0.1:50051",
		LayerDigests: []string{"sha256:aaa", "sha256:bbb"},
		CachedImages: []string{"nginx|latest|linux|amd64|"},
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
	if got := resp.GetServices()[0].GetNodeId(); got != "node-a" {
		t.Fatalf("node_id=%q, want node-a", got)
	}
	if len(resp.GetServices()[0].GetCachedImages()) != 1 {
		t.Fatalf("cached_images length=%d, want 1", len(resp.GetServices()[0].GetCachedImages()))
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
