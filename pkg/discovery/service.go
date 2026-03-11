package discovery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/registry"
)

const (
	defaultTTL             = 30 * time.Second
	defaultCleanupInterval = 5 * time.Second
	defaultImageCacheTTL   = 45 * time.Second
	defaultPlatformOS      = "linux"
	defaultPlatformArch    = "amd64"
)

type Resolver interface {
	GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error)
	Login(registry, username, password string) error
	LoginWithToken(registry, token string) error
}

type RegistryAuthConfig struct {
	RegistryHost string
	Username     string
	Password     string
	BearerToken  string
}

type imageRecord struct {
	imageKey           string
	resolvedRegistry   string
	resolvedRepository string
	resolvedReference  string
	layers             []registry.Layer
	expireAt           time.Time
}

type Service struct {
	discoverypb.UnimplementedServiceDiscoveryServer

	ttl             time.Duration
	cleanupInterval time.Duration
	imageCacheTTL   time.Duration
	now             func() time.Time

	newResolver   func() Resolver
	authByHost    map[string]RegistryAuthConfig
	mirrorsByHost map[string][]string

	mu         sync.RWMutex
	instances  map[string]serviceEntry
	byImageKey map[string]map[string]struct{}
	byLayer    map[string]map[string]struct{}
	images     map[string]imageRecord
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
		imageCacheTTL:   defaultImageCacheTTL,
		now:             time.Now,
		newResolver:     func() Resolver { return registry.NewClient(nil) },
		authByHost:      make(map[string]RegistryAuthConfig),
		mirrorsByHost:   make(map[string][]string),
		instances:       make(map[string]serviceEntry),
		byImageKey:      make(map[string]map[string]struct{}),
		byLayer:         make(map[string]map[string]struct{}),
		images:          make(map[string]imageRecord),
	}
	s.nextPrune = s.now().Add(s.cleanupInterval)
	go s.cleanupLoop(s.cleanupInterval)
	return s
}

func (s *Service) SetResolverFactory(fn func() Resolver) {
	if fn == nil {
		return
	}
	s.newResolver = fn
}

func (s *Service) SetRegistryAuthConfigs(authConfigs []RegistryAuthConfig) error {
	authByHost := make(map[string]RegistryAuthConfig, len(authConfigs))
	for _, cfg := range authConfigs {
		host := strings.TrimSpace(cfg.RegistryHost)
		if host == "" {
			return fmt.Errorf("registry host is required in auth config")
		}
		token := strings.TrimSpace(cfg.BearerToken)
		username := strings.TrimSpace(cfg.Username)
		if token != "" && username != "" {
			return fmt.Errorf("registry %s: bearer_token and username/password are mutually exclusive", host)
		}
		if username == "" && strings.TrimSpace(cfg.Password) != "" {
			return fmt.Errorf("registry %s: password requires username", host)
		}
		cfg.RegistryHost = host
		authByHost[host] = cfg
	}
	s.authByHost = authByHost
	return nil
}

func (s *Service) SetRegistryMirrors(m map[string][]string) {
	out := make(map[string][]string, len(m))
	for host, mirrors := range m {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		seen := make(map[string]struct{}, len(mirrors))
		cleaned := make([]string, 0, len(mirrors))
		for _, mirror := range mirrors {
			mirror = strings.TrimSpace(mirror)
			if mirror == "" {
				continue
			}
			if _, ok := seen[mirror]; ok {
				continue
			}
			seen[mirror] = struct{}{}
			cleaned = append(cleaned, mirror)
		}
		if len(cleaned) > 0 {
			out[host] = cleaned
		}
	}
	s.mirrorsByHost = out
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
	defer s.mu.Unlock()
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
	return &discoverypb.HeartbeatResponse{ExpiresInSeconds: int64(s.ttl / time.Second)}, nil
}

func (s *Service) ResolveImage(ctx context.Context, req *discoverypb.ResolveImageRequest) (*discoverypb.ResolveImageResponse, error) {
	if req == nil || strings.TrimSpace(req.GetImage()) == "" {
		return nil, status.Error(codes.InvalidArgument, "image is required")
	}
	platformOS := valueOrDefault(req.GetPlatformOs(), defaultPlatformOS)
	platformArch := valueOrDefault(req.GetPlatformArch(), defaultPlatformArch)
	platformVariant := strings.TrimSpace(req.GetPlatformVariant())
	key := imageKey(req.GetImage(), req.GetTag(), platformOS, platformArch, platformVariant)

	if !req.GetForceRefresh() {
		if cached, ok := s.loadCachedImage(key); ok {
			return imageRecordToProto(cached), nil
		}
	}

	ref, err := registry.ParseImageReference(req.GetImage(), req.GetTag())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse image reference: %v", err)
	}

	r := s.newResolver()
	if err := s.applyConfiguredAuth(r, ref.Registry); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "registry auth config: %v", err)
	}
	if err := s.applyConfiguredMirrors(r); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "registry mirror config: %v", err)
	}

	layers, err := r.GetLayersForPlatform(ctx, req.GetImage(), req.GetTag(), platformOS, platformArch, platformVariant)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve image layers: %v", err)
	}
	record := imageRecord{
		imageKey:           key,
		resolvedRegistry:   ref.Registry,
		resolvedRepository: ref.Repository,
		resolvedReference:  ref.Reference,
		layers:             append([]registry.Layer(nil), layers...),
		expireAt:           s.now().Add(s.imageCacheTTL),
	}

	s.mu.Lock()
	s.images[key] = record
	s.mu.Unlock()
	return imageRecordToProto(record), nil
}

func (s *Service) FindProvider(ctx context.Context, req *discoverypb.FindProviderRequest) (*discoverypb.FindProviderResponse, error) {
	_ = ctx
	layerDigest := ""
	if req != nil {
		layerDigest = strings.TrimSpace(req.GetLayerDigest())
	}
	s.maybePruneExpired()

	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := &discoverypb.FindProviderResponse{
		LayerDigest: layerDigest,
		Services:    make([]*discoverypb.ServiceInstance, 0, len(s.instances)),
	}
	if layerDigest == "" {
		for _, v := range s.instances {
			resp.Services = append(resp.Services, entryToProto(v))
		}
		return resp, nil
	}

	for endpoint := range s.layerProvidersLocked(layerDigest) {
		v, ok := s.instances[endpoint]
		if !ok {
			continue
		}
		resp.Services = append(resp.Services, entryToProto(v))
	}
	return resp, nil
}

func (s *Service) FindImageProvider(ctx context.Context, req *discoverypb.FindImageProviderRequest) (*discoverypb.FindImageProviderResponse, error) {
	_ = ctx
	imageKey := ""
	if req != nil {
		imageKey = strings.TrimSpace(req.GetImageKey())
	}
	if imageKey == "" {
		return nil, status.Error(codes.InvalidArgument, "image_key is required")
	}
	s.maybePruneExpired()

	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := &discoverypb.FindImageProviderResponse{
		ImageKey: imageKey,
		Services: make([]*discoverypb.ServiceInstance, 0, len(s.instances)),
	}
	for endpoint := range s.imageProvidersLocked(imageKey) {
		v, ok := s.instances[endpoint]
		if !ok {
			continue
		}
		resp.Services = append(resp.Services, entryToProto(v))
	}
	return resp, nil
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

	endpointSet := s.selectEndpointsLocked(imageKey, layerDigest)
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
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for endpoint, v := range s.instances {
		if v.lastSeen.Before(deadline) {
			s.removeEntryIndexes(endpoint, v)
			delete(s.instances, endpoint)
		}
	}
	for key, img := range s.images {
		if !now.Before(img.expireAt) {
			delete(s.images, key)
		}
	}
	s.nextPrune = s.now().Add(s.cleanupInterval)
}

func (s *Service) loadCachedImage(imageKey string) (imageRecord, bool) {
	now := s.now()
	s.mu.RLock()
	record, ok := s.images[imageKey]
	s.mu.RUnlock()
	if !ok || !now.Before(record.expireAt) {
		if ok {
			s.mu.Lock()
			delete(s.images, imageKey)
			s.mu.Unlock()
		}
		return imageRecord{}, false
	}
	return record, true
}

func imageRecordToProto(v imageRecord) *discoverypb.ResolveImageResponse {
	resp := &discoverypb.ResolveImageResponse{
		ImageKey:           v.imageKey,
		ResolvedRegistry:   v.resolvedRegistry,
		ResolvedRepository: v.resolvedRepository,
		ResolvedReference:  v.resolvedReference,
		Layers:             make([]*discoverypb.ImageLayer, 0, len(v.layers)),
	}
	for _, layer := range v.layers {
		resp.Layers = append(resp.Layers, &discoverypb.ImageLayer{
			Digest:         layer.Digest,
			MediaType:      layer.MediaType,
			CompressedSize: layer.Size,
		})
	}
	return resp
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

func (s *Service) selectEndpointsLocked(imageKey, layerDigest string) map[string]struct{} {
	imageKey = strings.TrimSpace(imageKey)
	layerDigest = strings.TrimSpace(layerDigest)
	if imageKey == "" && layerDigest == "" {
		return nil
	}
	if imageKey != "" && layerDigest != "" {
		return intersectSets(s.imageProvidersLocked(imageKey), s.byLayer[layerDigest])
	}
	if imageKey != "" {
		return s.imageProvidersLocked(imageKey)
	}
	return cloneSet(s.byLayer[layerDigest])
}

func (s *Service) layerProvidersLocked(layerDigest string) map[string]struct{} {
	layerDigest = strings.TrimSpace(layerDigest)
	if layerDigest == "" {
		return map[string]struct{}{}
	}
	return cloneSet(s.byLayer[layerDigest])
}

func (s *Service) imageProvidersLocked(imageKey string) map[string]struct{} {
	out := cloneSet(s.byImageKey[imageKey])
	complete := s.completeImageProvidersLocked(imageKey)
	for endpoint := range complete {
		out[endpoint] = struct{}{}
	}
	return out
}

func (s *Service) completeImageProvidersLocked(imageKey string) map[string]struct{} {
	record, ok := s.images[imageKey]
	if !ok || !s.now().Before(record.expireAt) {
		return map[string]struct{}{}
	}
	if len(record.layers) == 0 {
		return map[string]struct{}{}
	}

	var out map[string]struct{}
	seenDigests := make(map[string]struct{}, len(record.layers))
	for _, layer := range record.layers {
		digest := strings.TrimSpace(layer.Digest)
		if digest == "" {
			continue
		}
		if _, ok := seenDigests[digest]; ok {
			continue
		}
		seenDigests[digest] = struct{}{}

		providers := s.byLayer[digest]
		if len(providers) == 0 {
			return map[string]struct{}{}
		}
		if out == nil {
			out = cloneSet(providers)
			continue
		}
		out = intersectSets(out, providers)
		if len(out) == 0 {
			return out
		}
	}
	if out == nil {
		return map[string]struct{}{}
	}
	return out
}

func intersectSets(a, b map[string]struct{}) map[string]struct{} {
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
		return out
	}
	for ep := range b {
		if _, ok := a[ep]; ok {
			out[ep] = struct{}{}
		}
	}
	return out
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

func (s *Service) applyConfiguredAuth(r Resolver, registryHost string) error {
	auth, ok := s.authByHost[registryHost]
	if !ok {
		return nil
	}
	token := strings.TrimSpace(auth.BearerToken)
	username := strings.TrimSpace(auth.Username)
	password := auth.Password

	if token != "" && username != "" {
		return fmt.Errorf("bearer_token and username/password are mutually exclusive")
	}
	if username == "" && strings.TrimSpace(password) != "" {
		return fmt.Errorf("password requires username")
	}
	if token != "" {
		return r.LoginWithToken(registryHost, token)
	}
	if username != "" {
		return r.Login(registryHost, username, password)
	}
	return nil
}

func (s *Service) applyConfiguredMirrors(r Resolver) error {
	client, ok := r.(*registry.Client)
	if !ok {
		return nil
	}
	for host, mirrors := range s.mirrorsByHost {
		if err := client.SetRegistryMirrors(host, mirrors); err != nil {
			return err
		}
	}
	return nil
}

func valueOrDefault(v, d string) string {
	if s := strings.TrimSpace(v); s != "" {
		return s
	}
	return d
}

func imageKey(image, tag, platformOS, platformArch, platformVariant string) string {
	return strings.Join([]string{
		strings.TrimSpace(image),
		strings.TrimSpace(tag),
		strings.TrimSpace(platformOS),
		strings.TrimSpace(platformArch),
		strings.TrimSpace(platformVariant),
	}, "|")
}
