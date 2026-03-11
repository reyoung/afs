package afsproxy

import (
	"context"
	"io"
	"net"
	"strings"
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

func TestSortCandidatesByAvailableCache(t *testing.T) {
	t.Parallel()

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
	for i, got := range candidates {
		if got != want[i] {
			t.Fatalf("candidates[%d]=%q, want %q", i, got, want[i])
		}
	}
}

func TestMinLayerReplicaCountsPerLayer(t *testing.T) {
	t.Parallel()

	layers := []*discoverypb.ImageLayer{
		{Digest: "sha256:layer-a"},
		{Digest: "sha256:layer-b"},
	}
	services := []*discoverypb.ServiceInstance{
		{Endpoint: "node-a:50051", LayerDigests: []string{"sha256:layer-a"}},
		{Endpoint: "node-b:50051", LayerDigests: []string{"sha256:layer-b"}},
		{Endpoint: "node-c:50051", LayerDigests: []string{"sha256:layer-a", "sha256:layer-b"}},
	}

	if got := minLayerReplica(layers, services); got != 2 {
		t.Fatalf("minLayerReplica=%d, want 2", got)
	}
}

func TestPlanLayerReplicaAssignmentsDispersesLayers(t *testing.T) {
	t.Parallel()

	layers := []*discoverypb.ImageLayer{
		{Digest: "sha256:layer-a", CompressedSize: 100},
		{Digest: "sha256:layer-b", CompressedSize: 200},
	}
	services := []*discoverypb.ServiceInstance{
		{
			Endpoint:      "node-a:50051",
			CacheMaxBytes: 1000,
			LayerStats: []*discoverypb.LayerStat{
				{Digest: "sha256:layer-a", AfsSize: 100},
				{Digest: "sha256:layer-b", AfsSize: 200},
			},
		},
		{Endpoint: "node-b:50051", CacheMaxBytes: 700},
		{Endpoint: "node-c:50051", CacheMaxBytes: 600},
		{Endpoint: "node-d:50051", CacheMaxBytes: 500},
	}

	plan := planLayerReplicaAssignments(layers, 2, services)
	if got := len(plan); got < 2 {
		t.Fatalf("assigned endpoints=%d, want at least 2", got)
	}

	totalAssigned := 0
	maxPerNode := 0
	for endpoint, assigned := range plan {
		if endpoint == "node-a:50051" {
			t.Fatalf("existing full replica should not be selected again")
		}
		totalAssigned += len(assigned)
		if len(assigned) > maxPerNode {
			maxPerNode = len(assigned)
		}
	}
	if totalAssigned != len(layers) {
		t.Fatalf("assigned layers=%d, want %d", totalAssigned, len(layers))
	}
	if maxPerNode > 1 {
		t.Fatalf("expected dispersed assignments, got max_per_node=%d", maxPerNode)
	}
}

func TestReconcileImageReplica(t *testing.T) {
	t.Parallel()

	discAddr, stop := startDiscoveryServer(t)
	defer stop()
	layers := testResolvedLayers("nginx")
	heartbeat(t, discAddr, "node-a", "10.0.0.1:50051", layers)
	heartbeat(t, discAddr, "node-b", "10.0.0.2:50051", layers)
	heartbeat(t, discAddr, "node-c", "10.0.0.3:50051", []string{"sha256:other"})

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
	placements := placementTargetsByDigest(resp.GetLayers())
	if got := placements["sha256:nginx-a"]; strings.Join(got, ",") != "node-a@10.0.0.1:50051,node-b@10.0.0.2:50051" {
		t.Fatalf("layer-a placements=%v", got)
	}
	if got := placements["sha256:nginx-b"]; strings.Join(got, ",") != "node-a@10.0.0.1:50051,node-b@10.0.0.2:50051" {
		t.Fatalf("layer-b placements=%v", got)
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
	placements = placementTargetsByDigest(resp.GetLayers())
	if got := placements["sha256:nginx-a"]; len(got) != 2 {
		t.Fatalf("layer-a placements=%v, want 2 replicas", got)
	}
	if got := placements["sha256:nginx-b"]; len(got) != 2 {
		t.Fatalf("layer-b placements=%v, want 2 replicas", got)
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
	svc := discovery.NewService()
	svc.SetResolverFactory(func() discovery.Resolver {
		return &fakeDiscoveryResolver{}
	})
	s := grpc.NewServer()
	discoverypb.RegisterServiceDiscoveryServer(s, svc)
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
	layerStats := make([]*discoverypb.LayerStat, 0, len(layerDigests))
	for _, digest := range layerDigests {
		layerStats = append(layerStats, &discoverypb.LayerStat{Digest: digest, AfsSize: 1})
	}
	if _, err := client.Heartbeat(callCtx, &discoverypb.HeartbeatRequest{
		NodeId:       nodeID,
		Endpoint:     endpoint,
		LayerDigests: layerDigests,
		LayerStats:   layerStats,
	}); err != nil {
		t.Fatalf("heartbeat endpoint=%s: %v", endpoint, err)
	}
}

func placementTargetsByDigest(layers []*afsproxypb.ReconciledLayerPlacement) map[string][]string {
	out := make(map[string][]string, len(layers))
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		digest := layer.GetDigest()
		targets := make([]string, 0, len(layer.GetInstances()))
		for _, inst := range layer.GetInstances() {
			if inst == nil {
				continue
			}
			targets = append(targets, strings.TrimSpace(inst.GetNodeId())+"@"+strings.TrimSpace(inst.GetEndpoint()))
		}
		out[digest] = targets
	}
	return out
}

type fakeDiscoveryResolver struct{}

func (f *fakeDiscoveryResolver) GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error) {
	_ = ctx
	_ = tag
	_ = os
	_ = arch
	_ = variant
	return layersForImage(image), nil
}

func (f *fakeDiscoveryResolver) DownloadLayer(ctx context.Context, image, tag, digest string) (io.ReadCloser, error) {
	_ = ctx
	_ = image
	_ = tag
	_ = digest
	return nil, nil
}

func (f *fakeDiscoveryResolver) Login(registry, username, password string) error {
	_ = registry
	_ = username
	_ = password
	return nil
}

func (f *fakeDiscoveryResolver) LoginWithToken(registry, token string) error {
	_ = registry
	_ = token
	return nil
}

func testResolvedLayers(image string) []string {
	layers := layersForImage(image)
	out := make([]string, 0, len(layers))
	for _, layer := range layers {
		out = append(out, layer.Digest)
	}
	return out
}

func layersForImage(image string) []registry.Layer {
	switch image {
	case "busybox":
		return []registry.Layer{
			{Digest: "sha256:busybox-a", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 100},
			{Digest: "sha256:busybox-b", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 200},
		}
	default:
		return []registry.Layer{
			{Digest: "sha256:nginx-a", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 100},
			{Digest: "sha256:nginx-b", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 200},
		}
	}
}
