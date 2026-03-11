package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/reyoung/afs/pkg/afsproxypb"
	"google.golang.org/grpc"
)

type fakeProxyServer struct {
	afsproxypb.UnimplementedAfsProxyServer
}

func (s *fakeProxyServer) ReconcileImageReplica(ctx context.Context, req *afsproxypb.ReconcileImageReplicaRequest) (*afsproxypb.ReconcileImageReplicaResponse, error) {
	_ = ctx
	return &afsproxypb.ReconcileImageReplicaResponse{
		ImageKey:         req.GetImage() + "|" + req.GetTag() + "|" + req.GetPlatformOs() + "|" + req.GetPlatformArch() + "|" + req.GetPlatformVariant(),
		CurrentReplica:   2,
		RequestedReplica: req.GetReplica(),
		Ensured:          req.GetReplica() <= 2,
		Layers: []*afsproxypb.ReconciledLayerPlacement{
			{
				Digest: "sha256:layer-a",
				Instances: []*afsproxypb.LayerstoreTarget{
					{NodeId: "node-a", Endpoint: "10.0.0.1:50051"},
					{NodeId: "node-b", Endpoint: "10.0.0.2:50051"},
				},
			},
		},
	}, nil
}

func TestRunReconcileImageReplicaSubcommand(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	afsproxypb.RegisterAfsProxyServer(s, &fakeProxyServer{})
	go func() {
		_ = s.Serve(lis)
	}()
	defer func() {
		s.Stop()
		_ = lis.Close()
	}()

	var out bytes.Buffer
	err = runReconcileImageReplicaSubcommand([]string{
		"-addr", lis.Addr().String(),
		"-image", "nginx",
		"-tag", "latest",
		"-platform-os", "linux",
		"-platform-arch", "amd64",
		"-replica", "2",
	}, &out)
	if err != nil {
		t.Fatalf("runReconcileImageReplicaSubcommand: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "image_key=nginx|latest|linux|amd64|") {
		t.Fatalf("unexpected output: %q", got)
	}
	if !strings.Contains(got, "current_replica=2") {
		t.Fatalf("unexpected output: %q", got)
	}
	if !strings.Contains(got, "requested_replica=2") {
		t.Fatalf("unexpected output: %q", got)
	}
	if !strings.Contains(got, "ensured=true") {
		t.Fatalf("unexpected output: %q", got)
	}
	if !strings.Contains(got, "layer digest=sha256:layer-a") {
		t.Fatalf("unexpected output: %q", got)
	}
	if !strings.Contains(got, "instances=node-a@10.0.0.1:50051,node-b@10.0.0.2:50051") {
		t.Fatalf("unexpected output: %q", got)
	}
}
