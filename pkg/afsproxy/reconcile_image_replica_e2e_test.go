package afsproxy_test

import (
	"context"
	"net"
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

	imageKey := "nginx|latest|linux|amd64|"
	_, ls1, stopLS1 := startFakeLayerstoreE2E(t, discAddr, "node-a", []string{"sha256:aaa", "sha256:bbb"})
	defer stopLS1()
	_, ls2, stopLS2 := startFakeLayerstoreE2E(t, discAddr, "node-b", nil)
	defer stopLS2()

	proxyLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	proxyServer := grpc.NewServer()
	proxySvc := afsproxy.NewService(afsproxy.Config{
		DiscoveryTarget: discAddr,
		DialTimeout:     time.Second,
		StatusTimeout:   time.Second,
	})
	afsproxypb.RegisterAfsProxyServer(proxyServer, proxySvc)
	go func() {
		_ = proxyServer.Serve(proxyLis)
	}()
	defer func() {
		proxyServer.Stop()
		_ = proxyLis.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, proxyLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
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
	if got, want := resp.GetImageKey(), imageKey; got != want {
		t.Fatalf("image_key=%q, want %q", got, want)
	}
	if got := resp.GetCurrentReplica(); got < 2 {
		t.Fatalf("current_replica=%d, want >=2", got)
	}
	if !resp.GetEnsured() {
		t.Fatalf("ensured=%v, want true", resp.GetEnsured())
	}
	if !ls1.hasImage(imageKey) {
		t.Fatalf("layerstore-1 missing image after reconcile")
	}
	if !ls2.hasImage(imageKey) {
		t.Fatalf("layerstore-2 should pull image during reconcile")
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
}

type fakeLayerstoreServer struct {
	layerstorepb.UnimplementedLayerStoreServer

	discoveryAddr string
	nodeID        string
	endpoint      string

	mu     sync.RWMutex
	layers map[string]struct{}
}

func startFakeLayerstoreE2E(t *testing.T, discoveryAddr, nodeID string, initialLayers []string) (string, *fakeLayerstoreServer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen layerstore: %v", err)
	}
	srv := &fakeLayerstoreServer{
		discoveryAddr: discoveryAddr,
		nodeID:        nodeID,
		endpoint:      lis.Addr().String(),
		layers:        make(map[string]struct{}),
	}
	for _, k := range initialLayers {
		srv.layers[strings.TrimSpace(k)] = struct{}{}
	}

	grpcServer := grpc.NewServer()
	layerstorepb.RegisterLayerStoreServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	if err := srv.sendHeartbeat(context.Background()); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}
	stop := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}
	return lis.Addr().String(), srv, stop
}

func (s *fakeLayerstoreServer) EnsureLayers(ctx context.Context, req *layerstorepb.EnsureLayersRequest) (*layerstorepb.EnsureLayersResponse, error) {
	s.mu.Lock()
	for _, layer := range req.GetLayers() {
		if layer == nil {
			continue
		}
		s.layers[strings.TrimSpace(layer.GetDigest())] = struct{}{}
	}
	s.mu.Unlock()

	if err := s.sendHeartbeat(ctx); err != nil {
		return nil, err
	}
	return &layerstorepb.EnsureLayersResponse{}, nil
}

func (s *fakeLayerstoreServer) hasImage(imageKey string) bool {
	required := imageLayersForKey(imageKey)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, digest := range required {
		if _, ok := s.layers[digest]; !ok {
			return false
		}
	}
	return len(required) > 0
}

func (s *fakeLayerstoreServer) sendHeartbeat(ctx context.Context) error {
	dialCtx, dialCancel := context.WithTimeout(ctx, 2*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, s.discoveryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := discoverypb.NewServiceDiscoveryClient(conn)

	s.mu.RLock()
	layerDigests := make([]string, 0, len(s.layers))
	for k := range s.layers {
		layerDigests = append(layerDigests, k)
	}
	s.mu.RUnlock()

	callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
	defer callCancel()
	_, err = client.Heartbeat(callCtx, &discoverypb.HeartbeatRequest{
		NodeId:       s.nodeID,
		Endpoint:     s.endpoint,
		LayerDigests: layerDigests,
	})
	return err
}

func startDiscoveryE2E(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen discovery: %v", err)
	}
	s := grpc.NewServer()
	disc := discovery.NewService()
	disc.SetResolverFactory(func() discovery.Resolver {
		return e2eResolver{}
	})
	discoverypb.RegisterServiceDiscoveryServer(s, disc)
	go func() {
		_ = s.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

func imageLayersForKey(imageKey string) []string {
	switch strings.TrimSpace(imageKey) {
	case "nginx|latest|linux|amd64|":
		return []string{"sha256:aaa", "sha256:bbb"}
	case "busybox|latest|linux|amd64|":
		return []string{"sha256:ccc"}
	case "alpine|latest|linux|amd64|":
		return []string{"sha256:ddd"}
	default:
		return nil
	}
}

type e2eResolver struct{}

func (e2eResolver) GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error) {
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

func (e2eResolver) Login(registry, username, password string) error {
	_ = registry
	_ = username
	_ = password
	return nil
}

func (e2eResolver) LoginWithToken(registry, token string) error {
	_ = registry
	_ = token
	return nil
}
