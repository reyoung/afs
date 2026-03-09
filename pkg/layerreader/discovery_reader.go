package layerreader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

type ServiceInfo struct {
	NodeID   string
	Endpoint string
}

type Config struct {
	DiscoveryAddr string
	GRPCTimeout   time.Duration
	GRPCMaxChunk  int
	GRPCInsecure  bool
	NodeID        string
}

type DiscoveryBackedReaderAt struct {
	cfg       Config
	digest    string
	bootstrap string
	providers []ServiceInfo

	mu                   sync.Mutex
	nextRefreshAt        time.Time
	providerRefreshEvery time.Duration
	endpoint             string
	conn                 *grpc.ClientConn
	client               layerstorepb.LayerStoreClient
	size                 int64
}

func NewDiscoveryBackedLayerReader(cfg Config, digest string, bootstrapEndpoint string, providers []ServiceInfo) (*DiscoveryBackedReaderAt, error) {
	r := &DiscoveryBackedReaderAt{
		cfg:                  cfg,
		digest:               digest,
		bootstrap:            strings.TrimSpace(bootstrapEndpoint),
		providers:            append([]ServiceInfo(nil), providers...),
		providerRefreshEvery: 2 * time.Second,
	}
	if err := r.switchProvider(nil); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *DiscoveryBackedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}

	excluded := make(map[string]struct{})
	for {
		r.maybeRefreshProviders()
		client, size, endpoint := r.currentProvider()
		if client == nil {
			return 0, fmt.Errorf("layer provider is not initialized for digest=%s", r.digest)
		}
		if off >= size {
			return 0, io.EOF
		}
		maxReadable := int64(len(p))
		if off+maxReadable > size {
			maxReadable = size - off
		}
		total, err := r.readOnce(client, p[:maxReadable], off)
		if err == nil || errors.Is(err, io.EOF) {
			if int64(total) < int64(len(p)) {
				return total, io.EOF
			}
			return total, err
		}
		log.Printf("layer read failed, trying failover: digest=%s endpoint=%s err=%v", r.digest, endpoint, err)
		excluded[endpoint] = struct{}{}
		if switchErr := r.switchProvider(excluded); switchErr != nil {
			if total > 0 {
				return total, err
			}
			return 0, fmt.Errorf("read failed and no failover provider: %w", switchErr)
		}
	}
}

func (r *DiscoveryBackedReaderAt) maybeRefreshProviders() {
	now := time.Now()
	r.mu.Lock()
	if now.Before(r.nextRefreshAt) {
		r.mu.Unlock()
		return
	}
	r.nextRefreshAt = now.Add(r.providerRefreshEvery)
	r.mu.Unlock()

	fresh, err := findLayerServices(r.cfg.DiscoveryAddr, r.digest, r.cfg.GRPCTimeout, r.cfg.GRPCInsecure)
	if err != nil {
		return
	}
	if len(fresh) > 0 {
		r.mu.Lock()
		r.providers = fresh
		r.mu.Unlock()
	}
}

func (r *DiscoveryBackedReaderAt) currentProvider() (layerstorepb.LayerStoreClient, int64, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.client, r.size, r.endpoint
}

func (r *DiscoveryBackedReaderAt) readOnce(client layerstorepb.LayerStoreClient, p []byte, off int64) (int, error) {
	n, err := r.readOnceStream(client, p, off)
	if err == nil || errors.Is(err, io.EOF) {
		return n, err
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
		return r.readOnceUnary(client, p, off)
	}
	return n, err
}

func (r *DiscoveryBackedReaderAt) readOnceUnary(client layerstorepb.LayerStoreClient, p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		remaining := len(p) - total
		if remaining > r.cfg.GRPCMaxChunk {
			remaining = r.cfg.GRPCMaxChunk
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
		resp, err := client.ReadLayer(ctx, &layerstorepb.ReadLayerRequest{
			Digest: r.digest,
			Offset: off + int64(total),
			Length: int32(remaining),
		})
		cancel()
		if err != nil {
			return total, err
		}
		n := copy(p[total:], resp.GetData())
		total += n
		if n == 0 || resp.GetEof() {
			break
		}
	}
	if total < len(p) {
		return total, io.EOF
	}
	return total, nil
}

func (r *DiscoveryBackedReaderAt) readOnceStream(client layerstorepb.LayerStoreClient, p []byte, off int64) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
	defer cancel()
	stream, err := client.ReadLayerStream(ctx, &layerstorepb.ReadLayerRequest{
		Digest: r.digest,
		Offset: off,
		Length: int32(len(p)),
	})
	if err != nil {
		return 0, err
	}
	total := 0
	for total < len(p) {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			return total, recvErr
		}
		if resp == nil {
			break
		}
		n := copy(p[total:], resp.GetData())
		total += n
		if n == 0 || resp.GetEof() {
			break
		}
	}
	if total < len(p) {
		return total, io.EOF
	}
	return total, nil
}

func (r *DiscoveryBackedReaderAt) switchProvider(excluded map[string]struct{}) error {
	candidates := r.findLayerProviders()
	for _, c := range candidates {
		if _, skip := excluded[c.Endpoint]; skip {
			continue
		}
		if err := r.connectProvider(c.Endpoint); err != nil {
			log.Printf("provider connect failed: digest=%s endpoint=%s err=%v", r.digest, c.Endpoint, err)
			continue
		}
		log.Printf("layer provider selected: digest=%s endpoint=%s", r.digest, c.Endpoint)
		return nil
	}
	return fmt.Errorf("no available provider for digest=%s", r.digest)
}

func (r *DiscoveryBackedReaderAt) findLayerProviders() []ServiceInfo {
	r.mu.Lock()
	baseProviders := append([]ServiceInfo(nil), r.providers...)
	bootstrap := r.bootstrap
	r.mu.Unlock()

	candidates := make([]ServiceInfo, 0, len(baseProviders)+1)
	fresh, err := findLayerServices(r.cfg.DiscoveryAddr, r.digest, r.cfg.GRPCTimeout, r.cfg.GRPCInsecure)
	if err == nil && len(fresh) > 0 {
		r.mu.Lock()
		r.providers = fresh
		r.mu.Unlock()
		candidates = append(candidates, fresh...)
	} else {
		candidates = append(candidates, baseProviders...)
	}
	if bootstrap != "" {
		found := false
		for _, c := range candidates {
			if c.Endpoint == bootstrap {
				found = true
				break
			}
		}
		if !found {
			candidates = append(candidates, ServiceInfo{Endpoint: bootstrap})
		}
	}
	return rankServicesForAffinity(candidates, r.cfg.NodeID)
}

func findLayerServices(discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]ServiceInfo, error) {
	conn, err := dialGRPC(discoveryAddr, timeout, insecureTransport)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := discoverypb.NewServiceDiscoveryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := client.FindImage(ctx, &discoverypb.FindImageRequest{LayerDigest: digest})
	if err != nil {
		return nil, err
	}
	out := make([]ServiceInfo, 0, len(resp.GetServices()))
	for _, svc := range resp.GetServices() {
		if svc == nil {
			continue
		}
		out = append(out, ServiceInfo{
			NodeID:   strings.TrimSpace(svc.GetNodeId()),
			Endpoint: strings.TrimSpace(svc.GetEndpoint()),
		})
	}
	return dedupeServiceInfos(out), nil
}

func (r *DiscoveryBackedReaderAt) connectProvider(endpoint string) error {
	conn, err := dialGRPC(endpoint, r.cfg.GRPCTimeout, r.cfg.GRPCInsecure)
	if err != nil {
		return err
	}
	client := layerstorepb.NewLayerStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
	defer cancel()
	statResp, err := client.StatLayer(ctx, &layerstorepb.StatLayerRequest{Digest: r.digest})
	if err != nil {
		_ = conn.Close()
		return err
	}

	r.mu.Lock()
	oldConn := r.conn
	r.conn = conn
	r.client = client
	r.endpoint = endpoint
	r.size = statResp.GetAfsSize()
	r.mu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}
	return nil
}

func rankServicesForAffinity(services []ServiceInfo, nodeID string) []ServiceInfo {
	local := make([]ServiceInfo, 0, len(services))
	remote := make([]ServiceInfo, 0, len(services))
	for _, s := range services {
		if nodeID != "" && s.NodeID == nodeID {
			local = append(local, s)
		} else {
			remote = append(remote, s)
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(remote), func(i, j int) { remote[i], remote[j] = remote[j], remote[i] })
	r.Shuffle(len(local), func(i, j int) { local[i], local[j] = local[j], local[i] })
	return append(local, remote...)
}

func dedupeServiceInfos(in []ServiceInfo) []ServiceInfo {
	seen := make(map[string]struct{}, len(in))
	out := make([]ServiceInfo, 0, len(in))
	for _, s := range in {
		ep := strings.TrimSpace(s.Endpoint)
		if ep == "" {
			continue
		}
		if _, ok := seen[ep]; ok {
			continue
		}
		seen[ep] = struct{}{}
		out = append(out, ServiceInfo{NodeID: strings.TrimSpace(s.NodeID), Endpoint: ep})
	}
	return out
}

func dialGRPC(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("grpc address is required")
	}
	if !insecureTransport {
		return nil, fmt.Errorf("secure transport is not implemented")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}
