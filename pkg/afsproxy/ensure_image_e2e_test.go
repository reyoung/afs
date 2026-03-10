package afsproxy_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/afsproxy"
	"github.com/reyoung/afs/pkg/afsproxypb"
	"github.com/reyoung/afs/pkg/discovery"
	"github.com/reyoung/afs/pkg/discoverypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestEnsureImageE2E(t *testing.T) {
	t.Parallel()

	discAddr, stopDiscovery := startDiscoveryE2E(t)
	defer stopDiscovery()

	sendHeartbeatE2E(t, discAddr, "node-a", "10.0.0.1:50051", []string{"nginx|latest|linux|amd64|"})
	sendHeartbeatE2E(t, discAddr, "node-b", "10.0.0.2:50051", []string{"nginx|latest|linux|amd64|", "busybox|latest|linux|amd64|"})
	sendHeartbeatE2E(t, discAddr, "node-c", "10.0.0.3:50051", []string{"alpine|latest|linux|amd64|"})

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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, proxyLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	client := afsproxypb.NewAfsProxyClient(conn)
	resp, err := client.EnsureImage(ctx, &afsproxypb.EnsureImageRequest{
		Image:   "nginx",
		Tag:     "latest",
		Replica: 2,
	})
	if err != nil {
		t.Fatalf("EnsureImage: %v", err)
	}
	if got, want := resp.GetImageKey(), "nginx|latest|linux|amd64|"; got != want {
		t.Fatalf("image_key=%q, want %q", got, want)
	}
	if got := resp.GetCurrentReplica(); got != 2 {
		t.Fatalf("current_replica=%d, want 2", got)
	}
	if !resp.GetEnsured() {
		t.Fatalf("ensured=%v, want true", resp.GetEnsured())
	}

	resp, err = client.EnsureImage(ctx, &afsproxypb.EnsureImageRequest{
		Image:   "nginx",
		Tag:     "latest",
		Replica: 3,
	})
	if err != nil {
		t.Fatalf("EnsureImage(3): %v", err)
	}
	if got := resp.GetCurrentReplica(); got != 2 {
		t.Fatalf("current_replica=%d, want 2", got)
	}
	if resp.GetEnsured() {
		t.Fatalf("ensured=%v, want false", resp.GetEnsured())
	}
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

func sendHeartbeatE2E(t *testing.T, discoveryAddr, nodeID, endpoint string, cachedImages []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, discoveryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial discovery: %v", err)
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
		t.Fatalf("heartbeat %s: %v", endpoint, err)
	}
}
