package afsproxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/afsletpb"
	"github.com/reyoung/afs/pkg/afsproxypb"
	"github.com/reyoung/afs/pkg/discovery"
	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestCandidateLoad(t *testing.T) {
	t.Parallel()
	c := backendCandidate{status: &afsletpb.GetRuntimeStatusResponse{
		LimitCpuCores: 8,
		LimitMemoryMb: 1024,
		UsedCpuCores:  2,
		UsedMemoryMb:  768,
	}}
	got := candidateLoad(c)
	if got < 0.74 || got > 0.76 {
		t.Fatalf("candidateLoad=%f, want about 0.75", got)
	}
}

func TestPickByPowerOfTwoChoicesReturnsCandidate(t *testing.T) {
	t.Parallel()
	s := NewService(Config{AfsletTarget: "127.0.0.1:61051"})
	cands := []backendCandidate{
		{address: "a", status: &afsletpb.GetRuntimeStatusResponse{LimitCpuCores: 4, UsedCpuCores: 3, LimitMemoryMb: 400, UsedMemoryMb: 350}},
		{address: "b", status: &afsletpb.GetRuntimeStatusResponse{LimitCpuCores: 4, UsedCpuCores: 1, LimitMemoryMb: 400, UsedMemoryMb: 100}},
	}
	picked := s.pickByPowerOfTwoChoices(cands)
	if picked.address != "a" && picked.address != "b" {
		t.Fatalf("unexpected candidate: %s", picked.address)
	}
}

func TestComputeBackoff(t *testing.T) {
	t.Parallel()
	s := NewService(Config{})
	b1 := s.computeBackoff(50*time.Millisecond, 1)
	b2 := s.computeBackoff(50*time.Millisecond, 3)
	if b1 < 50*time.Millisecond {
		t.Fatalf("b1 too small: %s", b1)
	}
	if b2 < 150*time.Millisecond {
		t.Fatalf("b2 too small: %s", b2)
	}
}

func TestParseBoolDefaultTrue(t *testing.T) {
	t.Parallel()
	if !parseBoolDefaultTrue("1") {
		t.Fatalf("expected true for 1")
	}
	if !parseBoolDefaultTrue("true") {
		t.Fatalf("expected true for true")
	}
	if !parseBoolDefaultTrue("") {
		t.Fatalf("expected true for empty")
	}
	if parseBoolDefaultTrue("no") {
		t.Fatalf("expected false for no")
	}
}

func TestReconcileImageReplica(t *testing.T) {
	t.Parallel()

	discAddr, stop := startDiscoveryServer(t)
	defer stop()
	heartbeat(t, discAddr, "node-a", "10.0.0.1:50051", []string{"sha256:aaa", "sha256:bbb"})
	heartbeat(t, discAddr, "node-b", "10.0.0.2:50051", []string{"sha256:aaa", "sha256:bbb", "sha256:ccc"})
	heartbeat(t, discAddr, "node-c", "10.0.0.3:50051", []string{"sha256:ddd"})

	svc := NewService(Config{
		DiscoveryTarget: discAddr,
		DialTimeout:     time.Second,
		StatusTimeout:   time.Second,
	})

	resp, err := svc.ReconcileImageReplica(context.Background(), &afsproxypb.ReconcileImageReplicaRequest{
		Image:        "nginx",
		Tag:          "latest",
		PlatformOs:   "linux",
		PlatformArch: "amd64",
		Replica:      2,
	})
	if err != nil {
		t.Fatalf("ReconcileImageReplica: %v", err)
	}
	if resp.GetImageKey() != "nginx|latest|linux|amd64|" {
		t.Fatalf("image_key=%q, want nginx|latest|linux|amd64|", resp.GetImageKey())
	}
	if resp.GetCurrentReplica() != 2 {
		t.Fatalf("current_replica=%d, want 2", resp.GetCurrentReplica())
	}
	if !resp.GetEnsured() {
		t.Fatalf("ensured=%v, want true", resp.GetEnsured())
	}

	resp, err = svc.ReconcileImageReplica(context.Background(), &afsproxypb.ReconcileImageReplicaRequest{
		Image:        "nginx",
		Tag:          "latest",
		PlatformOs:   "linux",
		PlatformArch: "amd64",
		Replica:      3,
	})
	if err != nil {
		t.Fatalf("ReconcileImageReplica with higher replica: %v", err)
	}
	if resp.GetCurrentReplica() != 2 {
		t.Fatalf("current_replica=%d, want 2", resp.GetCurrentReplica())
	}
	if resp.GetEnsured() {
		t.Fatalf("ensured=%v, want false", resp.GetEnsured())
	}
}

func TestReconcileImageReplicaValidation(t *testing.T) {
	t.Parallel()

	svc := NewService(Config{})
	_, err := svc.ReconcileImageReplica(context.Background(), &afsproxypb.ReconcileImageReplicaRequest{
		Image:   "",
		Replica: 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code=%v, want %v", status.Code(err), codes.InvalidArgument)
	}

	_, err = svc.ReconcileImageReplica(context.Background(), &afsproxypb.ReconcileImageReplicaRequest{
		Image:   "nginx",
		Replica: -1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code=%v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func startDiscoveryServer(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	disc := discovery.NewService()
	disc.SetResolverFactory(func() discovery.Resolver {
		return proxyTestResolver{}
	})
	s := grpc.NewServer()
	discoverypb.RegisterServiceDiscoveryServer(s, disc)
	go func() {
		_ = s.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

func heartbeat(t *testing.T, discoveryAddr, nodeID, endpoint string, layerDigests []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, discoveryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial discovery %s: %v", discoveryAddr, err)
	}
	defer conn.Close()

	client := discoverypb.NewServiceDiscoveryClient(conn)
	callCtx, callCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer callCancel()
	if _, err := client.Heartbeat(callCtx, &discoverypb.HeartbeatRequest{
		NodeId:       nodeID,
		Endpoint:     endpoint,
		LayerDigests: layerDigests,
	}); err != nil {
		t.Fatalf("heartbeat endpoint=%s: %v", endpoint, err)
	}
}

type proxyTestResolver struct{}

func (proxyTestResolver) GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error) {
	_ = ctx
	_ = tag
	_ = os
	_ = arch
	_ = variant
	switch image {
	case "nginx":
		return []registry.Layer{
			{Digest: "sha256:aaa", MediaType: "layer/a", Size: 111},
			{Digest: "sha256:bbb", MediaType: "layer/b", Size: 222},
		}, nil
	case "busybox":
		return []registry.Layer{{Digest: "sha256:ccc", MediaType: "layer/c", Size: 333}}, nil
	case "alpine":
		return []registry.Layer{{Digest: "sha256:ddd", MediaType: "layer/d", Size: 444}}, nil
	default:
		return nil, nil
	}
}

func (proxyTestResolver) Login(registry, username, password string) error {
	_ = registry
	_ = username
	_ = password
	return nil
}

func (proxyTestResolver) LoginWithToken(registry, token string) error {
	_ = registry
	_ = token
	return nil
}
