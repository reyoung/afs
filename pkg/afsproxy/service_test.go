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

func TestSortCandidatesByAvailableCache(t *testing.T) {
	t.Parallel()

	// node-a: cache_max=100MB, used=0 → avail=100MB
	// node-b: cache_max=500MB, used=0 → avail=500MB
	// node-c: cache_max=300MB, used=100MB → avail=200MB
	// node-d: cache_max=0 → avail=0 (no cache info)
	svcByEndpoint := map[string]*discoverypb.ServiceInstance{
		"node-a:50051": {CacheMaxBytes: 100 * 1024 * 1024},
		"node-b:50051": {CacheMaxBytes: 500 * 1024 * 1024},
		"node-c:50051": {
			CacheMaxBytes: 300 * 1024 * 1024,
			LayerStats:    []*discoverypb.LayerStat{{AfsSize: 100 * 1024 * 1024}},
		},
		"node-d:50051": {CacheMaxBytes: 0},
	}

	candidates := []string{"node-a:50051", "node-b:50051", "node-c:50051", "node-d:50051"}
	sortCandidatesByAvailableCache(candidates, svcByEndpoint)

	want := []string{"node-b:50051", "node-c:50051", "node-a:50051", "node-d:50051"}
	for i, ep := range candidates {
		if ep != want[i] {
			t.Fatalf("candidates[%d]=%q, want %q (full order: %v)", i, ep, want[i], candidates)
		}
	}
}

func TestReconcileImageReplica(t *testing.T) {
	t.Parallel()

	discAddr, stop := startDiscoveryServer(t)
	defer stop()
	heartbeat(t, discAddr, "node-a", "10.0.0.1:50051", []string{"nginx|latest|linux|amd64|"})
	heartbeat(t, discAddr, "node-b", "10.0.0.2:50051", []string{"nginx|latest|linux|amd64|", "busybox|latest|linux|amd64|"})
	heartbeat(t, discAddr, "node-c", "10.0.0.3:50051", []string{"alpine|latest|linux|amd64|"})

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
	s := grpc.NewServer()
	discoverypb.RegisterServiceDiscoveryServer(s, discovery.NewService())
	go func() {
		_ = s.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

func heartbeat(t *testing.T, discoveryAddr, nodeID, endpoint string, cachedImages []string) {
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
		CachedImages: cachedImages,
	}); err != nil {
		t.Fatalf("heartbeat endpoint=%s: %v", endpoint, err)
	}
}
