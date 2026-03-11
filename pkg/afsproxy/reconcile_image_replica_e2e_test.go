package afsproxy_test

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/afsproxy"
	"github.com/reyoung/afs/pkg/afsproxypb"
	"github.com/reyoung/afs/pkg/discovery"
	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerstorepb"
	"github.com/reyoung/afs/pkg/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestReconcileImageReplicaE2E(t *testing.T) {
	t.Parallel()

	discAddr, stopDiscovery := startDiscoveryE2E(t)
	defer stopDiscovery()

	imageKey := imageKeyFor("nginx")
	targetLayers := resolvedLayersForImage("nginx")
	_, lsA, stopLSA := startFakeLayerstoreE2E(t, discAddr, "node-a", targetLayers, 0, 0)
	defer stopLSA()
	_, lsB, stopLSB := startFakeLayerstoreE2E(t, discAddr, "node-b", nil, 0, 0)
	defer stopLSB()

	proxyAddr, stopProxy := startProxyE2E(t, discAddr)
	defer stopProxy()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	client := afsproxypb.NewAfsProxyClient(conn)
	resp, err := client.ReconcileImageReplica(ctx, &afsproxypb.ReconcileImageReplicaRequest{
		Image:   "nginx",
		Tag:     "latest",
		Replica: 2,
	})
	if err != nil {
		t.Fatalf("ReconcileImageReplica: %v", err)
	}
	if got := resp.GetImageKey(); got != imageKey {
		t.Fatalf("image_key=%q, want %q", got, imageKey)
	}
	if got := resp.GetCurrentReplica(); got < 2 {
		t.Fatalf("current_replica=%d, want >=2", got)
	}
	if !resp.GetEnsured() {
		t.Fatalf("ensured=%v, want true", resp.GetEnsured())
	}
	placements := placementTargetsByDigestE2E(resp.GetLayers())
	for _, layer := range targetLayers {
		got := strings.Join(placements[layer.Digest], ",")
		want := strings.Join([]string{
			"node-a@" + lsA.endpoint,
			"node-b@" + lsB.endpoint,
		}, ",")
		if got != want {
			t.Fatalf("placements[%s]=%q, want %q", layer.Digest, got, want)
		}
	}
	if !lsA.hasAllLayers(targetLayers) {
		t.Fatalf("node-a missing layers after reconcile")
	}
	if !lsB.hasAllLayers(targetLayers) {
		t.Fatalf("node-b should cache all layers for replica=2")
	}

	resp, err = client.ReconcileImageReplica(ctx, &afsproxypb.ReconcileImageReplicaRequest{
		Image:   "nginx",
		Tag:     "latest",
		Replica: 3,
	})
	if err != nil {
		t.Fatalf("ReconcileImageReplica(3): %v", err)
	}
	if resp.GetEnsured() {
		t.Fatalf("ensured=%v, want false", resp.GetEnsured())
	}
	if got := resp.GetCurrentReplica(); got != 2 {
		t.Fatalf("current_replica=%d, want 2", got)
	}
	placements = placementTargetsByDigestE2E(resp.GetLayers())
	for _, layer := range targetLayers {
		if got := len(placements[layer.Digest]); got != 2 {
			t.Fatalf("placements[%s]=%v, want 2 replicas", layer.Digest, placements[layer.Digest])
		}
	}
}

func TestReconcileImageReplicaE2E_DistributesLayersAcrossNodes(t *testing.T) {
	t.Parallel()

	discAddr, stopDiscovery := startDiscoveryE2E(t)
	defer stopDiscovery()

	targetLayers := resolvedLayersForImage("busybox")
	_, lsA, stopLSA := startFakeLayerstoreE2E(t, discAddr, "node-a", targetLayers, 900, 0)
	defer stopLSA()
	_, lsB, stopLSB := startFakeLayerstoreE2E(t, discAddr, "node-b", nil, 700, 0)
	defer stopLSB()
	_, lsC, stopLSC := startFakeLayerstoreE2E(t, discAddr, "node-c", nil, 600, 0)
	defer stopLSC()
	_, lsD, stopLSD := startFakeLayerstoreE2E(t, discAddr, "node-d", nil, 500, 0)
	defer stopLSD()

	proxyAddr, stopProxy := startProxyE2E(t, discAddr)
	defer stopProxy()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	client := afsproxypb.NewAfsProxyClient(conn)
	resp, err := client.ReconcileImageReplica(ctx, &afsproxypb.ReconcileImageReplicaRequest{
		Image:   "busybox",
		Tag:     "latest",
		Replica: 2,
	})
	if err != nil {
		t.Fatalf("ReconcileImageReplica: %v", err)
	}
	if got := resp.GetCurrentReplica(); got < 2 {
		t.Fatalf("current_replica=%d, want >=2", got)
	}
	if !resp.GetEnsured() {
		t.Fatalf("ensured=%v, want true", resp.GetEnsured())
	}
	placements := placementTargetsByDigestE2E(resp.GetLayers())
	if !lsA.hasAllLayers(targetLayers) {
		t.Fatalf("node-a should keep original layers")
	}

	extraByNode := map[string]int{}
	for _, layer := range targetLayers {
		instances := placements[layer.Digest]
		if len(instances) != 2 {
			t.Fatalf("placements[%s]=%v, want 2 replicas", layer.Digest, instances)
		}
		foundOriginal := false
		for _, inst := range instances {
			switch inst {
			case "node-a@" + lsA.endpoint:
				foundOriginal = true
			case "node-b@" + lsB.endpoint:
				extraByNode["node-b"]++
			case "node-c@" + lsC.endpoint:
				extraByNode["node-c"]++
			case "node-d@" + lsD.endpoint:
				extraByNode["node-d"]++
			}
		}
		if !foundOriginal {
			t.Fatalf("placements[%s]=%v, want original replica on node-a", layer.Digest, instances)
		}
	}
	counts := []int{extraByNode["node-b"], extraByNode["node-c"], extraByNode["node-d"]}
	total := 0
	holders := 0
	maxPerNode := 0
	for _, count := range counts {
		total += count
		if count > 0 {
			holders++
		}
		if count > maxPerNode {
			maxPerNode = count
		}
	}
	if total != len(targetLayers) {
		t.Fatalf("new target layer copies=%d, want %d", total, len(targetLayers))
	}
	if holders < 2 {
		t.Fatalf("layers were not dispersed enough across nodes: counts=%v", counts)
	}
	if maxPerNode > 1 {
		t.Fatalf("expected each additional node to receive at most one target layer, counts=%v", counts)
	}
}

func TestReconcileImageReplicaE2E_DistributesLayersAcrossManyNodes(t *testing.T) {
	t.Parallel()

	discAddr, stopDiscovery := startDiscoveryE2E(t)
	defer stopDiscovery()

	targetLayers := resolvedLayersForImage("redis")
	_, lsA, stopLSA := startFakeLayerstoreE2E(t, discAddr, "node-a", targetLayers, 2400, 0)
	defer stopLSA()
	_, lsB, stopLSB := startFakeLayerstoreE2E(t, discAddr, "node-b", nil, 1500, 0)
	defer stopLSB()
	_, lsC, stopLSC := startFakeLayerstoreE2E(t, discAddr, "node-c", nil, 1400, 0)
	defer stopLSC()
	_, lsD, stopLSD := startFakeLayerstoreE2E(t, discAddr, "node-d", nil, 1300, 0)
	defer stopLSD()
	_, lsE, stopLSE := startFakeLayerstoreE2E(t, discAddr, "node-e", nil, 1200, 0)
	defer stopLSE()
	_, lsF, stopLSF := startFakeLayerstoreE2E(t, discAddr, "node-f", nil, 1100, 0)
	defer stopLSF()
	_, lsG, stopLSG := startFakeLayerstoreE2E(t, discAddr, "node-g", nil, 1000, 0)
	defer stopLSG()
	_, lsH, stopLSH := startFakeLayerstoreE2E(t, discAddr, "node-h", nil, 900, 0)
	defer stopLSH()

	proxyAddr, stopProxy := startProxyE2E(t, discAddr)
	defer stopProxy()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	client := afsproxypb.NewAfsProxyClient(conn)
	resp, err := client.ReconcileImageReplica(ctx, &afsproxypb.ReconcileImageReplicaRequest{
		Image:   "redis",
		Tag:     "latest",
		Replica: 2,
	})
	if err != nil {
		t.Fatalf("ReconcileImageReplica: %v", err)
	}
	if got := resp.GetCurrentReplica(); got < 2 {
		t.Fatalf("current_replica=%d, want >=2", got)
	}
	if !resp.GetEnsured() {
		t.Fatalf("ensured=%v, want true", resp.GetEnsured())
	}

	placements := placementTargetsByDigestE2E(resp.GetLayers())
	extraCounts := map[string]int{}
	for _, layer := range targetLayers {
		instances := placements[layer.Digest]
		if len(instances) != 2 {
			t.Fatalf("placements[%s]=%v, want 2 replicas", layer.Digest, instances)
		}
		foundOriginal := false
		for _, inst := range instances {
			switch inst {
			case "node-a@" + lsA.endpoint:
				foundOriginal = true
			case "node-b@" + lsB.endpoint:
				extraCounts["node-b"]++
			case "node-c@" + lsC.endpoint:
				extraCounts["node-c"]++
			case "node-d@" + lsD.endpoint:
				extraCounts["node-d"]++
			case "node-e@" + lsE.endpoint:
				extraCounts["node-e"]++
			case "node-f@" + lsF.endpoint:
				extraCounts["node-f"]++
			case "node-g@" + lsG.endpoint:
				extraCounts["node-g"]++
			case "node-h@" + lsH.endpoint:
				extraCounts["node-h"]++
			}
		}
		if !foundOriginal {
			t.Fatalf("placements[%s]=%v, want original replica on node-a", layer.Digest, instances)
		}
	}

	total := 0
	holders := 0
	maxPerNode := 0
	for _, node := range []string{"node-b", "node-c", "node-d", "node-e", "node-f", "node-g", "node-h"} {
		count := extraCounts[node]
		total += count
		if count > 0 {
			holders++
		}
		if count > maxPerNode {
			maxPerNode = count
		}
	}
	if total != len(targetLayers) {
		t.Fatalf("new target layer copies=%d, want %d", total, len(targetLayers))
	}
	if holders < len(targetLayers) {
		t.Fatalf("expected wide dispersion across nodes, holders=%d counts=%v", holders, extraCounts)
	}
	if maxPerNode > 1 {
		t.Fatalf("expected each additional node to receive at most one target layer, counts=%v", extraCounts)
	}
}

func startProxyE2E(t *testing.T, discoveryAddr string) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	server := grpc.NewServer()
	afsproxypb.RegisterAfsProxyServer(server, afsproxy.NewService(afsproxy.Config{
		DiscoveryTarget: discoveryAddr,
		DialTimeout:     time.Second,
		StatusTimeout:   time.Second,
	}))
	go func() {
		_ = server.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		server.Stop()
		_ = lis.Close()
	}
}

type fakeLayerstoreServer struct {
	layerstorepb.UnimplementedLayerStoreServer

	discoveryAddr string
	nodeID        string
	endpoint      string
	cacheMaxBytes int64
	baseUsedBytes int64

	mu     sync.RWMutex
	layers map[string]*layerstorepb.Layer
}

func startFakeLayerstoreE2E(t *testing.T, discoveryAddr, nodeID string, initialLayers []registry.Layer, cacheMaxBytes, baseUsedBytes int64) (string, *fakeLayerstoreServer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen layerstore: %v", err)
	}
	srv := &fakeLayerstoreServer{
		discoveryAddr: discoveryAddr,
		nodeID:        nodeID,
		endpoint:      lis.Addr().String(),
		cacheMaxBytes: cacheMaxBytes,
		baseUsedBytes: baseUsedBytes,
		layers:        make(map[string]*layerstorepb.Layer, len(initialLayers)),
	}
	for _, layer := range initialLayers {
		srv.layers[layer.Digest] = &layerstorepb.Layer{
			Digest:         layer.Digest,
			MediaType:      layer.MediaType,
			CompressedSize: layer.Size,
			AfsSize:        afsSizeForTest(layer.Size),
			CachePath:      "/fake/cache/" + sanitizeDigest(layer.Digest),
			Cached:         true,
		}
	}

	server := grpc.NewServer()
	layerstorepb.RegisterLayerStoreServer(server, srv)
	go func() {
		_ = server.Serve(lis)
	}()
	if err := srv.sendHeartbeat(context.Background()); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}
	return lis.Addr().String(), srv, func() {
		server.Stop()
		_ = lis.Close()
	}
}

func (s *fakeLayerstoreServer) EnsureLayers(ctx context.Context, req *layerstorepb.EnsureLayersRequest) (*layerstorepb.EnsureLayersResponse, error) {
	resp := &layerstorepb.EnsureLayersResponse{Layers: make([]*layerstorepb.Layer, 0, len(req.GetLayers()))}

	s.mu.Lock()
	for _, layer := range req.GetLayers() {
		if layer == nil {
			continue
		}
		digest := strings.TrimSpace(layer.GetDigest())
		if digest == "" {
			continue
		}
		existing, ok := s.layers[digest]
		if ok {
			cloned := *existing
			cloned.Cached = true
			resp.Layers = append(resp.Layers, &cloned)
			continue
		}
		stored := &layerstorepb.Layer{
			Digest:         digest,
			MediaType:      layer.GetMediaType(),
			CompressedSize: layer.GetCompressedSize(),
			AfsSize:        afsSizeForTest(layer.GetCompressedSize()),
			CachePath:      "/fake/cache/" + sanitizeDigest(digest),
			Cached:         false,
		}
		s.layers[digest] = stored
		cloned := *stored
		resp.Layers = append(resp.Layers, &cloned)
	}
	s.mu.Unlock()

	if err := s.sendHeartbeat(ctx); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *fakeLayerstoreServer) hasAllLayers(target []registry.Layer) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, layer := range target {
		if _, ok := s.layers[layer.Digest]; !ok {
			return false
		}
	}
	return true
}

func (s *fakeLayerstoreServer) targetLayerCount(target []registry.Layer) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, layer := range target {
		if _, ok := s.layers[layer.Digest]; ok {
			count++
		}
	}
	return count
}

func (s *fakeLayerstoreServer) sendHeartbeat(ctx context.Context) error {
	dialCtx, dialCancel := context.WithTimeout(ctx, 2*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, s.discoveryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	s.mu.RLock()
	layerDigests := make([]string, 0, len(s.layers))
	layerStats := make([]*discoverypb.LayerStat, 0, len(s.layers)+1)
	for digest, layer := range s.layers {
		layerDigests = append(layerDigests, digest)
		layerStats = append(layerStats, &discoverypb.LayerStat{
			Digest:  digest,
			AfsSize: layer.GetAfsSize(),
		})
	}
	s.mu.RUnlock()

	sort.Strings(layerDigests)
	sort.Slice(layerStats, func(i, j int) bool {
		return layerStats[i].GetDigest() < layerStats[j].GetDigest()
	})
	if s.baseUsedBytes > 0 {
		layerStats = append(layerStats, &discoverypb.LayerStat{
			Digest:  "used:" + s.nodeID,
			AfsSize: s.baseUsedBytes,
		})
	}

	client := discoverypb.NewServiceDiscoveryClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
	defer callCancel()
	_, err = client.Heartbeat(callCtx, &discoverypb.HeartbeatRequest{
		NodeId:        s.nodeID,
		Endpoint:      s.endpoint,
		LayerDigests:  layerDigests,
		LayerStats:    layerStats,
		CacheMaxBytes: s.cacheMaxBytes,
	})
	return err
}

func startDiscoveryE2E(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen discovery: %v", err)
	}
	svc := discovery.NewService()
	svc.SetResolverFactory(func() discovery.Resolver {
		return &fakeDiscoveryResolverE2E{}
	})
	server := grpc.NewServer()
	discoverypb.RegisterServiceDiscoveryServer(server, svc)
	go func() {
		_ = server.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		server.Stop()
		_ = lis.Close()
	}
}

type fakeDiscoveryResolverE2E struct{}

func (f *fakeDiscoveryResolverE2E) GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error) {
	_ = ctx
	_ = tag
	_ = os
	_ = arch
	_ = variant
	return append([]registry.Layer(nil), resolvedLayersForImage(image)...), nil
}

func (f *fakeDiscoveryResolverE2E) Login(registry, username, password string) error {
	_ = registry
	_ = username
	_ = password
	return nil
}

func (f *fakeDiscoveryResolverE2E) LoginWithToken(registry, token string) error {
	_ = registry
	_ = token
	return nil
}

func resolvedLayersForImage(image string) []registry.Layer {
	switch strings.TrimSpace(image) {
	case "busybox":
		return []registry.Layer{
			{Digest: "sha256:busybox-a", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 100},
			{Digest: "sha256:busybox-b", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 200},
		}
	case "redis":
		return []registry.Layer{
			{Digest: "sha256:redis-a", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 600},
			{Digest: "sha256:redis-b", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 500},
			{Digest: "sha256:redis-c", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 400},
			{Digest: "sha256:redis-d", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 300},
			{Digest: "sha256:redis-e", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 200},
			{Digest: "sha256:redis-f", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 100},
		}
	default:
		return []registry.Layer{
			{Digest: "sha256:nginx-a", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 100},
			{Digest: "sha256:nginx-b", MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 200},
		}
	}
}

func imageKeyFor(image string) string {
	return strings.Join([]string{strings.TrimSpace(image), "latest", "linux", "amd64", ""}, "|")
}

func afsSizeForTest(size int64) int64 {
	if size > 0 {
		return size
	}
	return 1
}

func sanitizeDigest(digest string) string {
	return strings.NewReplacer(":", "_", "/", "_").Replace(strings.TrimSpace(digest))
}

func placementTargetsByDigestE2E(layers []*afsproxypb.ReconciledLayerPlacement) map[string][]string {
	out := make(map[string][]string, len(layers))
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		targets := make([]string, 0, len(layer.GetInstances()))
		for _, inst := range layer.GetInstances() {
			if inst == nil {
				continue
			}
			targets = append(targets, strings.TrimSpace(inst.GetNodeId())+"@"+strings.TrimSpace(inst.GetEndpoint()))
		}
		out[layer.GetDigest()] = targets
	}
	return out
}
