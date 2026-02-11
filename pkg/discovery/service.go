package discovery

import (
	"context"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/discoverypb"
)

const (
	defaultTTL             = 30 * time.Second
	defaultCleanupInterval = 5 * time.Second
)

type Service struct {
	discoverypb.UnimplementedServiceDiscoveryServer

	ttl time.Duration
	now func() time.Time

	mu        sync.RWMutex
	instances map[string]serviceEntry
}

type serviceEntry struct {
	nodeID       string
	endpoint     string
	lastSeen     time.Time
	layerDigests []string
	layerStats   map[string]int64
	cachedImages []string
	cacheMax     int64
}

func NewService() *Service {
	s := &Service{
		ttl:       defaultTTL,
		now:       time.Now,
		instances: make(map[string]serviceEntry),
	}
	go s.cleanupLoop(defaultCleanupInterval)
	return s
}

func (s *Service) Heartbeat(ctx context.Context, req *discoverypb.HeartbeatRequest) (*discoverypb.HeartbeatResponse, error) {
	_ = ctx
	nodeID := strings.TrimSpace(req.GetNodeId())
	endpoint := strings.TrimSpace(req.GetEndpoint())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if endpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "endpoint is required")
	}

	layers := dedupeNonEmpty(req.GetLayerDigests())
	layerStats := dedupeLayerStats(req.GetLayerStats())
	if len(layerStats) > 0 {
		layers = mergeLayerDigestLists(layers, layerStats)
	}
	now := s.now()

	s.mu.Lock()
	s.instances[endpoint] = serviceEntry{
		nodeID:       nodeID,
		endpoint:     endpoint,
		lastSeen:     now,
		layerDigests: layers,
		layerStats:   layerStats,
		cachedImages: dedupeNonEmpty(req.GetCachedImages()),
		cacheMax:     req.GetCacheMaxBytes(),
	}
	s.mu.Unlock()

	return &discoverypb.HeartbeatResponse{ExpiresInSeconds: int64(s.ttl / time.Second)}, nil
}

func (s *Service) FindImage(ctx context.Context, req *discoverypb.FindImageRequest) (*discoverypb.FindImageResponse, error) {
	_ = ctx
	imageKey := ""
	if req != nil {
		imageKey = strings.TrimSpace(req.GetImageKey())
	}
	s.pruneExpired()

	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := &discoverypb.FindImageResponse{ImageKey: imageKey, Services: make([]*discoverypb.ServiceInstance, 0, len(s.instances))}
	for _, v := range s.instances {
		if imageKey != "" && !contains(v.cachedImages, imageKey) {
			continue
		}
		resp.Services = append(resp.Services, entryToProto(v))
	}
	return resp, nil
}

func (s *Service) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.pruneExpired()
	}
}

func (s *Service) pruneExpired() {
	deadline := s.now().Add(-s.ttl)
	s.mu.Lock()
	defer s.mu.Unlock()
	for endpoint, v := range s.instances {
		if v.lastSeen.Before(deadline) {
			delete(s.instances, endpoint)
		}
	}
}

func entryToProto(v serviceEntry) *discoverypb.ServiceInstance {
	layers := make([]string, len(v.layerDigests))
	copy(layers, v.layerDigests)
	layerStats := make([]*discoverypb.LayerStat, 0, len(v.layerStats))
	for _, digest := range layers {
		layerStats = append(layerStats, &discoverypb.LayerStat{
			Digest:  digest,
			AfsSize: v.layerStats[digest],
		})
	}
	return &discoverypb.ServiceInstance{
		NodeId:        v.nodeID,
		Endpoint:      v.endpoint,
		LastSeenUnix:  v.lastSeen.Unix(),
		LayerDigests:  layers,
		CachedImages:  append([]string(nil), v.cachedImages...),
		LayerStats:    layerStats,
		CacheMaxBytes: v.cacheMax,
	}
}

func dedupeNonEmpty(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		s := strings.TrimSpace(v)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func contains(arr []string, target string) bool {
	for _, v := range arr {
		if v == target {
			return true
		}
	}
	return false
}

func dedupeLayerStats(in []*discoverypb.LayerStat) map[string]int64 {
	out := make(map[string]int64, len(in))
	for _, ls := range in {
		if ls == nil {
			continue
		}
		d := strings.TrimSpace(ls.GetDigest())
		if d == "" {
			continue
		}
		out[d] = ls.GetAfsSize()
	}
	return out
}

func mergeLayerDigestLists(digests []string, stats map[string]int64) []string {
	seen := make(map[string]struct{}, len(digests)+len(stats))
	out := make([]string, 0, len(digests)+len(stats))
	for _, d := range digests {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	for d := range stats {
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}
