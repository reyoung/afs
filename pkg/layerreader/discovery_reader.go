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
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

const (
	defaultLayerLeaseTTL      = 2 * time.Minute
	defaultLayerLeaseRenewTTL = 30 * time.Second
)

type ServiceInfo struct {
	NodeID   string
	Endpoint string
}

type Config struct {
	DiscoveryAddr        string
	GRPCTimeout          time.Duration
	GRPCMaxChunk         int
	GRPCInsecure         bool
	NodeID               string
	KnownLayerAfsSize    int64
	ProviderRefreshEvery time.Duration
	DialGRPCAcquire      func(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, func(), error)
	FindLayerServices    func(discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]ServiceInfo, error)
}

type DiscoveryBackedReaderAt struct {
	cfg               Config
	digest            string
	bootstrap         string
	providers         []ServiceInfo
	dialGRPCAcquire   func(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, func(), error)
	findLayerServices func(discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]ServiceInfo, error)
	switchMu          sync.Mutex

	mu                   sync.Mutex
	nextRefreshAt        time.Time
	providerRefreshEvery time.Duration
	endpoint             string
	conn                 *grpc.ClientConn
	connRelease          func()
	client               layerstorepb.LayerStoreClient
	size                 int64
	leaseID              string
	renewCancel          context.CancelFunc
	closed               bool
}

func NewDiscoveryBackedLayerReader(cfg Config, digest string, bootstrapEndpoint string, providers []ServiceInfo) (*DiscoveryBackedReaderAt, error) {
	dialer := cfg.DialGRPCAcquire
	if dialer == nil {
		dialer = defaultDialGRPCAcquire
	}
	finder := cfg.FindLayerServices
	if finder == nil {
		finder = findLayerServices
	}
	refreshEvery := cfg.ProviderRefreshEvery
	if refreshEvery <= 0 {
		refreshEvery = 30 * time.Second
	}
	r := &DiscoveryBackedReaderAt{
		cfg:                  cfg,
		digest:               digest,
		bootstrap:            strings.TrimSpace(bootstrapEndpoint),
		providers:            append([]ServiceInfo(nil), providers...),
		providerRefreshEvery: refreshEvery,
		dialGRPCAcquire:      dialer,
		findLayerServices:    finder,
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
		client, size, endpoint, closed := r.currentProvider()
		if client == nil {
			if closed {
				return 0, fmt.Errorf("layer reader is closed for digest=%s", r.digest)
			}
			if err := r.switchProvider(excluded); err != nil {
				return 0, fmt.Errorf("layer provider is not initialized for digest=%s: %w", r.digest, err)
			}
			continue
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
	if r.providerRefreshEvery <= 0 {
		return
	}
	now := time.Now()
	r.mu.Lock()
	if now.Before(r.nextRefreshAt) {
		r.mu.Unlock()
		return
	}
	r.nextRefreshAt = now.Add(r.providerRefreshEvery)
	r.mu.Unlock()

	fresh, err := r.findLayerServices(r.cfg.DiscoveryAddr, r.digest, r.cfg.GRPCTimeout, r.cfg.GRPCInsecure)
	if err != nil {
		return
	}
	if len(fresh) > 0 {
		r.mu.Lock()
		r.providers = fresh
		r.mu.Unlock()
	}
}

func (r *DiscoveryBackedReaderAt) currentProvider() (layerstorepb.LayerStoreClient, int64, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.client, r.size, r.endpoint, r.closed
}

func (r *DiscoveryBackedReaderAt) readOnce(client layerstorepb.LayerStoreClient, p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		remaining := len(p) - total
		if remaining > r.cfg.GRPCMaxChunk {
			remaining = r.cfg.GRPCMaxChunk
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
		stream, err := client.ReadLayerStream(ctx, &layerstorepb.ReadLayerRequest{
			Digest: r.digest,
			Offset: off + int64(total),
			Length: int32(remaining),
		})
		if err != nil {
			cancel()
			return total, err
		}
		chunkTotal := 0
		for chunkTotal < remaining {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				cancel()
				if errors.Is(recvErr, io.EOF) {
					break
				}
				return total + chunkTotal, recvErr
			}
			if resp == nil {
				break
			}
			n := copy(p[total+chunkTotal:], resp.GetData())
			chunkTotal += n
			if n == 0 || resp.GetEof() {
				break
			}
		}
		cancel()
		total += chunkTotal
		if chunkTotal < remaining {
			break
		}
	}
	if total < len(p) {
		return total, io.EOF
	}
	return total, nil
}

func (r *DiscoveryBackedReaderAt) switchProvider(excluded map[string]struct{}) error {
	r.switchMu.Lock()
	defer r.switchMu.Unlock()

	candidates := r.currentCandidates()
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
	fresh, err := r.findLayerServices(r.cfg.DiscoveryAddr, r.digest, r.cfg.GRPCTimeout, r.cfg.GRPCInsecure)
	if err == nil && len(fresh) > 0 {
		r.mu.Lock()
		r.providers = fresh
		r.mu.Unlock()
		for _, c := range rankServicesForAffinity(append([]ServiceInfo(nil), fresh...), r.cfg.NodeID) {
			if _, skip := excluded[c.Endpoint]; skip {
				continue
			}
			if err := r.connectProvider(c.Endpoint); err != nil {
				log.Printf("provider connect failed after refresh: digest=%s endpoint=%s err=%v", r.digest, c.Endpoint, err)
				continue
			}
			log.Printf("layer provider selected after refresh: digest=%s endpoint=%s", r.digest, c.Endpoint)
			return nil
		}
	}

	return fmt.Errorf("no available provider for digest=%s", r.digest)
}

func (r *DiscoveryBackedReaderAt) currentCandidates() []ServiceInfo {
	r.mu.Lock()
	baseProviders := append([]ServiceInfo(nil), r.providers...)
	bootstrap := r.bootstrap
	r.mu.Unlock()

	candidates := make([]ServiceInfo, 0, len(baseProviders)+1)
	candidates = append(candidates, baseProviders...)
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
	return rankServicesForAffinity(dedupeServiceInfos(candidates), r.cfg.NodeID)
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
	resp, err := client.FindProvider(ctx, &discoverypb.FindProviderRequest{LayerDigest: digest})
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
	conn, release, err := r.dialGRPCAcquire(endpoint, r.cfg.GRPCTimeout, r.cfg.GRPCInsecure)
	if err != nil {
		return err
	}
	client := layerstorepb.NewLayerStoreClient(conn)
	leaseID, err := r.acquireLease(client, endpoint)
	if err != nil {
		release()
		return err
	}
	size := r.cfg.KnownLayerAfsSize
	if size <= 0 {
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
		statResp, err := client.StatLayer(ctx, &layerstorepb.StatLayerRequest{Digest: r.digest})
		cancel()
		if err != nil {
			r.releaseLease(client, endpoint, leaseID)
			release()
			return err
		}
		size = statResp.GetAfsSize()
	}
	renewCancel := r.startLeaseRenewLoop(client, endpoint, leaseID)

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		renewCancel()
		r.releaseLease(client, endpoint, leaseID)
		release()
		return fmt.Errorf("layer reader is closed for digest=%s", r.digest)
	}
	oldConn := r.conn
	oldRelease := r.connRelease
	oldClient := r.client
	oldEndpoint := r.endpoint
	oldLeaseID := r.leaseID
	oldRenewCancel := r.renewCancel
	r.conn = conn
	r.connRelease = release
	r.client = client
	r.endpoint = endpoint
	r.size = size
	r.leaseID = leaseID
	r.renewCancel = renewCancel
	r.mu.Unlock()

	if oldRenewCancel != nil {
		oldRenewCancel()
	}
	if oldClient != nil && oldLeaseID != "" {
		r.releaseLease(oldClient, oldEndpoint, oldLeaseID)
	}
	if oldRelease != nil {
		oldRelease()
	} else if oldConn != nil {
		_ = oldConn.Close()
	}
	return nil
}

func (r *DiscoveryBackedReaderAt) Close() error {
	r.mu.Lock()
	conn := r.conn
	release := r.connRelease
	client := r.client
	endpoint := r.endpoint
	leaseID := r.leaseID
	renewCancel := r.renewCancel
	r.conn = nil
	r.connRelease = nil
	r.client = nil
	r.endpoint = ""
	r.leaseID = ""
	r.renewCancel = nil
	r.closed = true
	r.mu.Unlock()

	if renewCancel != nil {
		renewCancel()
	}
	if client != nil && leaseID != "" {
		r.releaseLease(client, endpoint, leaseID)
	}
	if release != nil {
		release()
		return nil
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (r *DiscoveryBackedReaderAt) acquireLease(client layerstorepb.LayerStoreClient, endpoint string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
	defer cancel()
	resp, err := client.AcquireLayerLease(ctx, &layerstorepb.AcquireLayerLeaseRequest{
		Digests: []string{r.digest},
		TtlMs:   defaultLayerLeaseTTL.Milliseconds(),
	})
	if err != nil {
		log.Printf("layer lease acquire failed: digest=%s endpoint=%s err=%v", r.digest, endpoint, err)
		return "", err
	}
	return strings.TrimSpace(resp.GetLeaseId()), nil
}

func (r *DiscoveryBackedReaderAt) releaseLease(client layerstorepb.LayerStoreClient, endpoint string, leaseID string) {
	if client == nil || strings.TrimSpace(leaseID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
	defer cancel()
	if _, err := client.ReleaseLayerLease(ctx, &layerstorepb.ReleaseLayerLeaseRequest{LeaseId: leaseID}); err != nil {
		log.Printf("layer lease release failed: digest=%s endpoint=%s lease_id=%s err=%v", r.digest, endpoint, leaseID, err)
	}
}

func (r *DiscoveryBackedReaderAt) startLeaseRenewLoop(client layerstorepb.LayerStoreClient, endpoint string, leaseID string) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(defaultLayerLeaseRenewTTL)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				callCtx, callCancel := context.WithTimeout(context.Background(), r.cfg.GRPCTimeout)
				_, err := client.RenewLayerLease(callCtx, &layerstorepb.RenewLayerLeaseRequest{
					LeaseId: leaseID,
					TtlMs:   defaultLayerLeaseTTL.Milliseconds(),
				})
				callCancel()
				if err != nil {
					r.onLeaseRenewFailure(endpoint, leaseID, err)
					return
				}
			}
		}
	}()
	return cancel
}

func (r *DiscoveryBackedReaderAt) onLeaseRenewFailure(endpoint string, leaseID string, err error) {
	log.Printf("layer lease renew failed: digest=%s endpoint=%s lease_id=%s err=%v", r.digest, endpoint, leaseID, err)

	r.mu.Lock()
	if r.closed || r.endpoint != endpoint || r.leaseID != leaseID {
		r.mu.Unlock()
		return
	}
	conn := r.conn
	release := r.connRelease
	client := r.client
	renewCancel := r.renewCancel
	r.conn = nil
	r.connRelease = nil
	r.client = nil
	r.endpoint = ""
	r.size = 0
	r.leaseID = ""
	r.renewCancel = nil
	r.mu.Unlock()

	if renewCancel != nil {
		renewCancel()
	}
	if client != nil {
		r.releaseLease(client, endpoint, leaseID)
	}
	if release != nil {
		release()
	} else if conn != nil {
		_ = conn.Close()
	}
	go func() {
		if switchErr := r.switchProvider(map[string]struct{}{endpoint: {}}); switchErr != nil {
			log.Printf("layer provider failover after lease renew failure failed: digest=%s endpoint=%s err=%v", r.digest, endpoint, switchErr)
		}
	}()
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

func defaultDialGRPCAcquire(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, func(), error) {
	conn, err := dialGRPC(addr, timeout, insecureTransport)
	if err != nil {
		return nil, nil, err
	}
	return conn, func() { _ = conn.Close() }, nil
}
