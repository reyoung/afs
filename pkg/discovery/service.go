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

	ttl             time.Duration
	cleanupInterval time.Duration
	now             func() time.Time

	mu         sync.RWMutex
	instances  map[string]serviceEntry
	byImageKey map[string]map[string]struct{}
	byLayer    map[string]map[string]struct{}
	nextPrune  time.Time
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
		ttl:             defaultTTL,
		cleanupInterval: defaultCleanupInterval,
		now:             time.Now,
		instances:       make(map[string]serviceEntry),
		byImageKey:      make(map[string]map[string]struct{}),
		byLayer:         make(map[string]map[string]struct{}),
	}
	s.nextPrune = s.now().Add(s.cleanupInterval)
	go s.cleanupLoop(s.cleanupInterval)
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
	if prev, ok := s.instances[endpoint]; ok {
		s.removeEntryIndexes(endpoint, prev)
	}
	s.instances[endpoint] = serviceEntry{
		nodeID:       nodeID,
		endpoint:     endpoint,
		lastSeen:     now,
		layerDigests: layers,
		layerStats:   layerStats,
		cachedImages: dedupeNonEmpty(req.GetCachedImages()),
		cacheMax:     req.GetCacheMaxBytes(),
	}
	s.addEntryIndexes(endpoint, s.instances[endpoint])
	s.mu.Unlock()

	return &discoverypb.HeartbeatResponse{ExpiresInSeconds: int64(s.ttl / time.Second)}, nil
}

func (s *Service) FindImage(ctx context.Context, req *discoverypb.FindImageRequest) (*discoverypb.FindImageResponse, error) {
	_ = ctx
	imageKey := ""
	layerDigest := ""
	if req != nil {
		imageKey = strings.TrimSpace(req.GetImageKey())
		layerDigest = strings.TrimSpace(req.GetLayerDigest())
	}
	s.maybePruneExpired()

	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := &discoverypb.FindImageResponse{
		ImageKey: imageKey,
		Services: make([]*discoverypb.ServiceInstance, 0, len(s.instances)),
	}

	endpointSet := s.selectEndpoints(imageKey, layerDigest)
	if endpointSet == nil {
		for _, v := range s.instances {
			resp.Services = append(resp.Services, entryToProto(v))
		}
		return resp, nil
	}

	for endpoint := range endpointSet {
		v, ok := s.instances[endpoint]
		if !ok {
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

func (s *Service) maybePruneExpired() {
	now := s.now()
	s.mu.RLock()
	due := !now.Before(s.nextPrune)
	s.mu.RUnlock()
	if due {
		s.pruneExpired()
	}
}

func (s *Service) pruneExpired() {
	deadline := s.now().Add(-s.ttl)
	s.mu.Lock()
	defer s.mu.Unlock()
	for endpoint, v := range s.instances {
		if v.lastSeen.Before(deadline) {
			s.removeEntryIndexes(endpoint, v)
			delete(s.instances, endpoint)
		}
	}
	s.nextPrune = s.now().Add(s.cleanupInterval)
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

func (s *Service) addEntryIndexes(endpoint string, v serviceEntry) {
	for _, k := range v.cachedImages {
		addIndex(s.byImageKey, k, endpoint)
	}
	for _, d := range v.layerDigests {
		addIndex(s.byLayer, d, endpoint)
	}
}

func (s *Service) removeEntryIndexes(endpoint string, v serviceEntry) {
	for _, k := range v.cachedImages {
		removeIndex(s.byImageKey, k, endpoint)
	}
	for _, d := range v.layerDigests {
		removeIndex(s.byLayer, d, endpoint)
	}
}

func addIndex(idx map[string]map[string]struct{}, key, endpoint string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	s, ok := idx[key]
	if !ok {
		s = make(map[string]struct{})
		idx[key] = s
	}
	s[endpoint] = struct{}{}
}

func removeIndex(idx map[string]map[string]struct{}, key, endpoint string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	s, ok := idx[key]
	if !ok {
		return
	}
	delete(s, endpoint)
	if len(s) == 0 {
		delete(idx, key)
	}
}

func (s *Service) selectEndpoints(imageKey, layerDigest string) map[string]struct{} {
	imageKey = strings.TrimSpace(imageKey)
	layerDigest = strings.TrimSpace(layerDigest)
	if imageKey == "" && layerDigest == "" {
		return nil
	}
	if imageKey != "" && layerDigest != "" {
		a := s.byImageKey[imageKey]
		b := s.byLayer[layerDigest]
		if len(a) == 0 || len(b) == 0 {
			return map[string]struct{}{}
		}
		out := make(map[string]struct{})
		if len(a) < len(b) {
			for ep := range a {
				if _, ok := b[ep]; ok {
					out[ep] = struct{}{}
				}
			}
		} else {
			for ep := range b {
				if _, ok := a[ep]; ok {
					out[ep] = struct{}{}
				}
			}
		}
		return out
	}
	if imageKey != "" {
		return cloneSet(s.byImageKey[imageKey])
	}
	return cloneSet(s.byLayer[layerDigest])
}

func cloneSet(in map[string]struct{}) map[string]struct{} {
	if len(in) == 0 {
		return map[string]struct{}{}
	}
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
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
