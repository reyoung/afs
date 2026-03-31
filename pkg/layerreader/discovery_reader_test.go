package layerreader

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/reyoung/afs/pkg/layerstorepb"
)

type fakeLeaseLayerStore struct {
	layerstorepb.UnimplementedLayerStoreServer

	mu           sync.Mutex
	digest       string
	data         []byte
	acquireCount int
	releaseCount int
	renewCount   int
}

func (s *fakeLeaseLayerStore) StatLayer(context.Context, *layerstorepb.StatLayerRequest) (*layerstorepb.StatLayerResponse, error) {
	return &layerstorepb.StatLayerResponse{
		Digest:  s.digest,
		AfsSize: int64(len(s.data)),
	}, nil
}

func (s *fakeLeaseLayerStore) ReadLayerStream(req *layerstorepb.ReadLayerRequest, stream layerstorepb.LayerStore_ReadLayerStreamServer) error {
	start := int(req.GetOffset())
	if start >= len(s.data) {
		return nil
	}
	end := start + int(req.GetLength())
	if end > len(s.data) {
		end = len(s.data)
	}
	return stream.Send(&layerstorepb.ReadLayerResponse{
		Digest: req.GetDigest(),
		Offset: req.GetOffset(),
		Data:   append([]byte(nil), s.data[start:end]...),
		Eof:    end == len(s.data),
	})
}

func (s *fakeLeaseLayerStore) AcquireLayerLease(context.Context, *layerstorepb.AcquireLayerLeaseRequest) (*layerstorepb.AcquireLayerLeaseResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireCount++
	return &layerstorepb.AcquireLayerLeaseResponse{
		LeaseId:         "lease-id",
		Digests:         []string{s.digest},
		ExpiresAtUnixMs: time.Now().Add(2 * time.Minute).UnixMilli(),
	}, nil
}

func (s *fakeLeaseLayerStore) RenewLayerLease(context.Context, *layerstorepb.RenewLayerLeaseRequest) (*layerstorepb.RenewLayerLeaseResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewCount++
	return &layerstorepb.RenewLayerLeaseResponse{
		LeaseId:         "lease-id",
		Digests:         []string{s.digest},
		ExpiresAtUnixMs: time.Now().Add(2 * time.Minute).UnixMilli(),
	}, nil
}

func (s *fakeLeaseLayerStore) ReleaseLayerLease(context.Context, *layerstorepb.ReleaseLayerLeaseRequest) (*layerstorepb.ReleaseLayerLeaseResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCount++
	return &layerstorepb.ReleaseLayerLeaseResponse{Released: true}, nil
}

func TestDiscoveryBackedReaderLeaseLifecycle(t *testing.T) {
	t.Parallel()

	const endpoint = "bufnet"
	digest := "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	server := &fakeLeaseLayerStore{
		digest: digest,
		data:   []byte("hello world"),
	}
	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	layerstorepb.RegisterLayerStoreServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	reader, err := NewDiscoveryBackedLayerReader(Config{
		DiscoveryAddr: "unused",
		GRPCTimeout:   time.Second,
		GRPCMaxChunk:  4,
		GRPCInsecure:  true,
		DialGRPCAcquire: func(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, func(), error) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			conn, err := grpc.DialContext(ctx, addr,
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return lis.Dial()
				}),
				grpc.WithInsecure(),
				grpc.WithBlock(),
			)
			cancel()
			if err != nil {
				return nil, nil, err
			}
			return conn, func() { _ = conn.Close() }, nil
		},
		FindLayerServices: func(discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]ServiceInfo, error) {
			return []ServiceInfo{{Endpoint: endpoint}}, nil
		},
	}, digest, endpoint, []ServiceInfo{{Endpoint: endpoint}})
	if err != nil {
		t.Fatalf("NewDiscoveryBackedLayerReader: %v", err)
	}

	buf := make([]byte, 5)
	n, err := reader.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
	}
	if got := string(buf[:n]); got != "hello" {
		t.Fatalf("ReadAt got %q", got)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.acquireCount == 0 {
		t.Fatalf("expected lease acquire")
	}
	if server.releaseCount == 0 {
		t.Fatalf("expected lease release on close")
	}
}

func TestDiscoveryBackedReaderRecoversWhenProviderStateIsCleared(t *testing.T) {
	t.Parallel()

	const endpoint = "bufnet"
	digest := "sha256:abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	server := &fakeLeaseLayerStore{
		digest: digest,
		data:   []byte("abcdef"),
	}
	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	layerstorepb.RegisterLayerStoreServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	dialer := func(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, func(), error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithInsecure(),
			grpc.WithBlock(),
		)
		cancel()
		if err != nil {
			return nil, nil, err
		}
		return conn, func() { _ = conn.Close() }, nil
	}
	reader, err := NewDiscoveryBackedLayerReader(Config{
		DiscoveryAddr:   "unused",
		GRPCTimeout:     time.Second,
		GRPCMaxChunk:    8,
		GRPCInsecure:    true,
		DialGRPCAcquire: dialer,
		FindLayerServices: func(discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]ServiceInfo, error) {
			return []ServiceInfo{{Endpoint: endpoint}}, nil
		},
	}, digest, endpoint, []ServiceInfo{{Endpoint: endpoint}})
	if err != nil {
		t.Fatalf("NewDiscoveryBackedLayerReader: %v", err)
	}
	defer reader.Close()

	reader.mu.Lock()
	client := reader.client
	leaseID := reader.leaseID
	release := reader.connRelease
	renewCancel := reader.renewCancel
	reader.conn = nil
	reader.connRelease = nil
	reader.client = nil
	reader.endpoint = ""
	reader.size = 0
	reader.leaseID = ""
	reader.renewCancel = nil
	reader.mu.Unlock()
	if renewCancel != nil {
		renewCancel()
	}
	if client != nil && leaseID != "" {
		reader.releaseLease(client, endpoint, leaseID)
	}
	if release != nil {
		release()
	}

	buf := make([]byte, 6)
	n, err := reader.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt after provider reset: %v", err)
	}
	if got := string(buf[:n]); got != "abcdef" {
		t.Fatalf("ReadAt after provider reset got %q", got)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.acquireCount < 2 {
		t.Fatalf("expected provider reacquire, got acquire_count=%d", server.acquireCount)
	}
}
