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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestReconcileImageReplicaE2E(t *testing.T) {
	t.Parallel()

	discAddr, stopDiscovery := startDiscoveryE2E(t)
	defer stopDiscovery()

	imageKey := "nginx|latest|linux|amd64|"
	_, ls1, stopLS1 := startFakeLayerstoreE2E(t, discAddr, "node-a", []string{imageKey})
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
	cached map[string]struct{}
}

func startFakeLayerstoreE2E(t *testing.T, discoveryAddr, nodeID string, initialImages []string) (string, *fakeLayerstoreServer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen layerstore: %v", err)
	}
	srv := &fakeLayerstoreServer{
		discoveryAddr: discoveryAddr,
		nodeID:        nodeID,
		endpoint:      lis.Addr().String(),
		cached:        make(map[string]struct{}),
	}
	for _, k := range initialImages {
		srv.cached[strings.TrimSpace(k)] = struct{}{}
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

func (s *fakeLayerstoreServer) PullImage(ctx context.Context, req *layerstorepb.PullImageRequest) (*layerstorepb.PullImageResponse, error) {
	imageKey := strings.Join([]string{
		strings.TrimSpace(req.GetImage()),
		strings.TrimSpace(req.GetTag()),
		valueOrDefault(req.GetPlatformOs(), "linux"),
		valueOrDefault(req.GetPlatformArch(), "amd64"),
		strings.TrimSpace(req.GetPlatformVariant()),
	}, "|")

	s.mu.Lock()
	s.cached[imageKey] = struct{}{}
	s.mu.Unlock()

	if err := s.sendHeartbeat(ctx); err != nil {
		return nil, err
	}
	return &layerstorepb.PullImageResponse{}, nil
}

func (s *fakeLayerstoreServer) hasImage(imageKey string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.cached[strings.TrimSpace(imageKey)]
	return ok
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
	cached := make([]string, 0, len(s.cached))
	for k := range s.cached {
		cached = append(cached, k)
	}
	s.mu.RUnlock()

	callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
	defer callCancel()
	_, err = client.Heartbeat(callCtx, &discoverypb.HeartbeatRequest{
		NodeId:       s.nodeID,
		Endpoint:     s.endpoint,
		CachedImages: cached,
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
	discoverypb.RegisterServiceDiscoveryServer(s, discovery.NewService())
	go func() {
		_ = s.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

func valueOrDefault(v, d string) string {
	if s := strings.TrimSpace(v); s != "" {
		return s
	}
	return d
}
