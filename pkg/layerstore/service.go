package layerstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerstorepb"
	"github.com/reyoung/afs/pkg/registry"
)

const (
	maxReadBytes        = 4 << 20
	defaultCacheMaxByte = int64(1) << 40 // 1TB
	defaultCacheRatio   = int64(60)      // 60% of free space
)

var digestPattern = regexp.MustCompile(`^[A-Za-z0-9_+.-]+:[A-Fa-f0-9]+$`)

type fetcher interface {
	DownloadLayerFromRepository(ctx context.Context, registryHost, repository, digest string) (io.ReadCloser, error)
	Login(registry, username, password string) error
	LoginWithToken(registry, token string) error
}

type RegistryAuthConfig struct {
	RegistryHost string
	Username     string
	Password     string
	BearerToken  string
}

type Service struct {
	layerstorepb.UnimplementedLayerStoreServer

	cacheDir      string
	layersDir     string
	locker        *digestLocker
	newFetcher    func() fetcher
	maxReadSize   int
	authByHost    map[string]RegistryAuthConfig
	mirrorsByHost map[string][]string
	pruneMu       sync.Mutex
	cacheMax      int64
	reservedBytes int64
	cacheChangeReport func()
}

type cachedLayerFile struct {
	digest  string
	path    string
	size    int64
	modTime time.Time
}

func NewService(cacheDir string, authConfigs []RegistryAuthConfig) (*Service, error) {
	if strings.TrimSpace(cacheDir) == "" {
		return nil, fmt.Errorf("cache directory is required")
	}
	layersDir := filepath.Join(cacheDir, "layers")
	if err := os.MkdirAll(layersDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	authByHost := make(map[string]RegistryAuthConfig)
	for _, cfg := range authConfigs {
		host := strings.TrimSpace(cfg.RegistryHost)
		if host == "" {
			return nil, fmt.Errorf("registry host is required in auth config")
		}
		token := strings.TrimSpace(cfg.BearerToken)
		username := strings.TrimSpace(cfg.Username)
		if token != "" && username != "" {
			return nil, fmt.Errorf("registry %s: bearer_token and username/password are mutually exclusive", host)
		}
		if username == "" && strings.TrimSpace(cfg.Password) != "" {
			return nil, fmt.Errorf("registry %s: password requires username", host)
		}
		cfg.RegistryHost = host
		authByHost[host] = cfg
	}
	cacheMax := defaultCacheMaxByte
	if dyn, err := defaultCacheLimitFromFS(cacheDir); err == nil && dyn > 0 && dyn < cacheMax {
		cacheMax = dyn
	}
	return &Service{
		cacheDir:         cacheDir,
		layersDir:        layersDir,
		locker:           newDigestLocker(),
		newFetcher:       func() fetcher { return registry.NewClient(nil) },
		maxReadSize:      maxReadBytes,
		authByHost:       authByHost,
		mirrorsByHost:    make(map[string][]string),
		cacheMax:         cacheMax,
	}, nil
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

func (s *Service) EnsureLayers(ctx context.Context, req *layerstorepb.EnsureLayersRequest) (*layerstorepb.EnsureLayersResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	source := req.GetSource()
	if source == nil {
		return nil, status.Error(codes.InvalidArgument, "source is required")
	}
	registryHost := strings.TrimSpace(source.GetRegistry())
	repository := strings.TrimSpace(source.GetRepository())
	if registryHost == "" {
		return nil, status.Error(codes.InvalidArgument, "source.registry is required")
	}
	if repository == "" {
		return nil, status.Error(codes.InvalidArgument, "source.repository is required")
	}

	layers := make([]registry.Layer, 0, len(req.GetLayers()))
	for _, spec := range req.GetLayers() {
		if spec == nil {
			continue
		}
		digest := strings.TrimSpace(spec.GetDigest())
		if digest == "" {
			return nil, status.Error(codes.InvalidArgument, "layer digest is required")
		}
		layers = append(layers, registry.Layer{
			Digest:    digest,
			MediaType: strings.TrimSpace(spec.GetMediaType()),
			Size:      spec.GetCompressedSize(),
		})
	}
	if len(layers) == 0 {
		return &layerstorepb.EnsureLayersResponse{}, nil
	}

	f := s.newFetcher()
	if err := s.applyConfiguredAuth(f, registryHost); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "registry auth config: %v", err)
	}
	if err := s.applyConfiguredMirrors(f); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "registry mirror config: %v", err)
	}

	releaseReservation, reserveErr := s.reservePullSpace(layers)
	if reserveErr != nil {
		return nil, reserveErr
	}
	defer releaseReservation()

	result := &layerstorepb.EnsureLayersResponse{
		Layers: make([]*layerstorepb.Layer, 0, len(layers)),
	}
	for i, layer := range layers {
		log.Printf("[%d/%d] ensuring layer digest=%s mediaType=%s size=%d registry=%s repository=%s", i+1, len(layers), layer.Digest, layer.MediaType, layer.Size, registryHost, repository)
		cachePath, afsSize, cached, err := s.ensureLayer(ctx, f, registryHost, repository, layer, true)
		if err != nil {
			return nil, err
		}
		if cached {
			log.Printf("[%d/%d] layer cache hit digest=%s afsSize=%d path=%s", i+1, len(layers), layer.Digest, afsSize, cachePath)
		} else {
			log.Printf("[%d/%d] layer cached digest=%s afsSize=%d path=%s", i+1, len(layers), layer.Digest, afsSize, cachePath)
		}
		result.Layers = append(result.Layers, &layerstorepb.Layer{
			Digest:         layer.Digest,
			MediaType:      layer.MediaType,
			CompressedSize: layer.Size,
			AfsSize:        afsSize,
			CachePath:      cachePath,
			Cached:         cached,
		})
	}
	return result, nil
}

func (s *Service) SetCacheChangeReporter(fn func()) {
	s.cacheChangeReport = fn
}

func (s *Service) SetCacheLimitBytes(v int64) {
	if v <= 0 {
		return
	}
	s.cacheMax = v
}

func (s *Service) CacheLimitBytes() int64 {
	return s.cacheMax
}

func (s *Service) reservePullSpace(layers []registry.Layer) (func(), error) {
	s.pruneMu.Lock()
	defer s.pruneMu.Unlock()

	required, protected, err := s.estimateRequiredBytesLocked(layers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "estimate required space: %v", err)
	}
	if required > s.cacheMax {
		return nil, status.Errorf(codes.ResourceExhausted, "image requires %d bytes but cache limit is %d", required, s.cacheMax)
	}

	usage, err := s.cacheUsageBytesLocked()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "compute cache usage: %v", err)
	}
	available := s.cacheMax - usage - s.reservedBytes
	if available < required {
		target := required - available
		reclaimed, _, pruneErr := s.pruneByReclaimTargetLocked(target, protected)
		if pruneErr != nil {
			return nil, status.Errorf(codes.Internal, "prune for reservation: %v", pruneErr)
		}
		available += reclaimed
	}
	if available < required {
		return nil, status.Errorf(codes.ResourceExhausted, "insufficient cache space: need=%d available=%d limit=%d", required, available, s.cacheMax)
	}

	s.reservedBytes += required
	return func() {
		s.pruneMu.Lock()
		defer s.pruneMu.Unlock()
		s.reservedBytes -= required
		if s.reservedBytes < 0 {
			s.reservedBytes = 0
		}
	}, nil
}

func (s *Service) estimateRequiredBytesLocked(layers []registry.Layer) (int64, map[string]struct{}, error) {
	protected := make(map[string]struct{}, len(layers))
	var required int64
	for _, l := range layers {
		d := strings.TrimSpace(l.Digest)
		if d == "" {
			continue
		}
		protected[d] = struct{}{}
		p, err := s.layerPath(d)
		if err != nil {
			return 0, nil, err
		}
		if _, statErr := os.Stat(p); statErr == nil {
			continue
		}
		if l.Size > 0 {
			required += l.Size
		} else {
			required += 1
		}
	}
	return required, protected, nil
}

func (s *Service) StatLayer(ctx context.Context, req *layerstorepb.StatLayerRequest) (*layerstorepb.StatLayerResponse, error) {
	_ = ctx
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	fullPath, err := s.layerPath(req.GetDigest())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	st, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "layer is not cached: %s", req.GetDigest())
		}
		return nil, status.Errorf(codes.Internal, "stat layer: %v", err)
	}
	_ = touchFile(fullPath)
	return &layerstorepb.StatLayerResponse{
		Digest:    req.GetDigest(),
		AfsSize:   st.Size(),
		CachePath: fullPath,
	}, nil
}

func (s *Service) ReadLayer(ctx context.Context, req *layerstorepb.ReadLayerRequest) (*layerstorepb.ReadLayerResponse, error) {
	_ = ctx
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetOffset() < 0 {
		return nil, status.Error(codes.InvalidArgument, "offset must be >= 0")
	}
	if req.GetLength() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "length must be > 0")
	}
	if int(req.GetLength()) > s.maxReadSize {
		return nil, status.Errorf(codes.InvalidArgument, "length exceeds max: %d", s.maxReadSize)
	}

	fullPath, err := s.layerPath(req.GetDigest())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	f, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "layer is not cached: %s", req.GetDigest())
		}
		return nil, status.Errorf(codes.Internal, "open layer: %v", err)
	}
	defer f.Close()

	buf := make([]byte, int(req.GetLength()))
	n, readErr := f.ReadAt(buf, req.GetOffset())
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, status.Errorf(codes.Internal, "read layer: %v", readErr)
	}
	_ = touchFile(fullPath)

	return &layerstorepb.ReadLayerResponse{
		Digest: req.GetDigest(),
		Offset: req.GetOffset(),
		Data:   buf[:n],
		Eof:    errors.Is(readErr, io.EOF),
	}, nil
}

func (s *Service) ReadLayerStream(req *layerstorepb.ReadLayerRequest, stream layerstorepb.LayerStore_ReadLayerStreamServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetOffset() < 0 {
		return status.Error(codes.InvalidArgument, "offset must be >= 0")
	}
	if req.GetLength() <= 0 {
		return status.Error(codes.InvalidArgument, "length must be > 0")
	}
	if int(req.GetLength()) > s.maxReadSize {
		return status.Errorf(codes.InvalidArgument, "length exceeds max: %d", s.maxReadSize)
	}

	fullPath, err := s.layerPath(req.GetDigest())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	f, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return status.Errorf(codes.NotFound, "layer is not cached: %s", req.GetDigest())
		}
		return status.Errorf(codes.Internal, "open layer: %v", err)
	}
	defer f.Close()

	remaining := int(req.GetLength())
	curOffset := req.GetOffset()
	for remaining > 0 {
		chunk := remaining
		if chunk > s.maxReadSize {
			chunk = s.maxReadSize
		}
		buf := make([]byte, chunk)
		n, readErr := f.ReadAt(buf, curOffset)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return status.Errorf(codes.Internal, "read layer: %v", readErr)
		}
		resp := &layerstorepb.ReadLayerResponse{
			Digest: req.GetDigest(),
			Offset: curOffset,
			Data:   buf[:n],
			Eof:    errors.Is(readErr, io.EOF),
		}
		if sendErr := stream.Send(resp); sendErr != nil {
			return sendErr
		}
		curOffset += int64(n)
		remaining -= n
		if n == 0 || errors.Is(readErr, io.EOF) {
			break
		}
	}
	_ = touchFile(fullPath)
	return nil
}

func (s *Service) HasLayer(ctx context.Context, req *layerstorepb.HasLayerRequest) (*layerstorepb.HasLayerResponse, error) {
	_ = ctx
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	fullPath, err := s.layerPath(req.GetDigest())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	st, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &layerstorepb.HasLayerResponse{Digest: req.GetDigest(), Found: false}, nil
		}
		return nil, status.Errorf(codes.Internal, "stat layer: %v", err)
	}
	return &layerstorepb.HasLayerResponse{
		Digest:  req.GetDigest(),
		Found:   true,
		AfsSize: st.Size(),
	}, nil
}

func (s *Service) PruneCache(ctx context.Context, req *layerstorepb.PruneCacheRequest) (*layerstorepb.PruneCacheResponse, error) {
	_ = ctx
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	percent := req.GetPercent()
	if percent <= 0 || percent > 100 {
		return nil, status.Error(codes.InvalidArgument, "percent must be in (0,100]")
	}
	before, err := s.cacheUsageBytes()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "compute cache usage: %v", err)
	}
	target := int64(float64(before) * (percent / 100.0))
	if target <= 0 {
		return &layerstorepb.PruneCacheResponse{
			BeforeBytes:    before,
			AfterBytes:     before,
			ReclaimedBytes: 0,
			EvictedLayers:  0,
		}, nil
	}
	reclaimed, evicted, pruneErr := s.pruneByReclaimTarget(target, nil)
	if pruneErr != nil {
		return nil, status.Errorf(codes.Internal, "prune cache: %v", pruneErr)
	}
	after := before - reclaimed
	if after < 0 {
		after = 0
	}
	return &layerstorepb.PruneCacheResponse{
		BeforeBytes:    before,
		AfterBytes:     after,
		ReclaimedBytes: reclaimed,
		EvictedLayers:  int32(evicted),
	}, nil
}

func (s *Service) ensureLayer(ctx context.Context, f fetcher, registryHost, repository string, layer registry.Layer, allowDownload bool) (cachePath string, size int64, cached bool, err error) {
	if !layerformat.IsSupportedLayerMediaType(layer.MediaType) {
		return "", 0, false, status.Errorf(codes.FailedPrecondition, "unsupported layer media type %q for %s", layer.MediaType, layer.Digest)
	}

	fullPath, err := s.layerPath(layer.Digest)
	if err != nil {
		return "", 0, false, status.Error(codes.InvalidArgument, err.Error())
	}

	if st, statErr := os.Stat(fullPath); statErr == nil {
		_ = touchFile(fullPath)
		return fullPath, st.Size(), true, nil
	}
	if !allowDownload {
		return "", 0, false, status.Errorf(codes.NotFound, "layer is not cached: %s", layer.Digest)
	}

	unlock := s.locker.lock(layer.Digest)
	defer unlock()

	if st, statErr := os.Stat(fullPath); statErr == nil {
		_ = touchFile(fullPath)
		return fullPath, st.Size(), true, nil
	}

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "create layer cache dir: %v", err)
	}

	rc, err := f.DownloadLayerFromRepository(ctx, registryHost, repository, layer.Digest)
	if err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "download layer %s: %v", layer.Digest, err)
	}
	defer rc.Close()
	log.Printf("downloading layer digest=%s from registry=%s repository=%s", layer.Digest, registryHost, repository)

	tmpFile, err := os.CreateTemp(dir, ".tmp-*.afslyr")
	if err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	if err = layerformat.ConvertOCILayer(layer.MediaType, rc, tmpFile); err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "convert layer %s: %v", layer.Digest, err)
	}
	log.Printf("converted layer digest=%s to afs temp=%s", layer.Digest, tmpPath)
	if err = tmpFile.Close(); err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "close temp file: %v", err)
	}
	if err = os.Rename(tmpPath, fullPath); err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "commit cache file: %v", err)
	}

	st, err := os.Stat(fullPath)
	if err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "stat cache file: %v", err)
	}
	_ = touchFile(fullPath)
	if s.cacheChangeReport != nil {
		s.cacheChangeReport()
	}
	if _, _, pruneErr := s.enforceCacheLimit(map[string]struct{}{layer.Digest: {}}); pruneErr != nil {
		log.Printf("warning: enforce cache limit failed: %v", pruneErr)
	}
	return fullPath, st.Size(), false, nil
}

func (s *Service) enforceCacheLimit(exclude map[string]struct{}) (int64, int, error) {
	if s.cacheMax <= 0 {
		return 0, 0, nil
	}
	s.pruneMu.Lock()
	defer s.pruneMu.Unlock()

	usage, err := s.cacheUsageBytesLocked()
	if err != nil {
		return 0, 0, err
	}
	if usage+s.reservedBytes <= s.cacheMax {
		return 0, 0, nil
	}
	target := usage + s.reservedBytes - s.cacheMax
	return s.pruneByReclaimTargetLocked(target, exclude)
}

func (s *Service) pruneByReclaimTarget(target int64, exclude map[string]struct{}) (int64, int, error) {
	if target <= 0 {
		return 0, 0, nil
	}
	s.pruneMu.Lock()
	defer s.pruneMu.Unlock()
	return s.pruneByReclaimTargetLocked(target, exclude)
}

func (s *Service) pruneByReclaimTargetLocked(target int64, exclude map[string]struct{}) (int64, int, error) {
	files, _, err := s.scanLayerFiles()
	if err != nil {
		return 0, 0, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})

	var reclaimed int64
	evicted := 0
	removed := false
	for _, f := range files {
		if reclaimed >= target {
			break
		}
		if exclude != nil {
			if _, ok := exclude[f.digest]; ok {
				continue
			}
		}
		if err := os.Remove(f.path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return reclaimed, evicted, err
		}
		reclaimed += f.size
		evicted++
		removed = true
	}
	if removed && s.cacheChangeReport != nil {
		s.cacheChangeReport()
	}
	return reclaimed, evicted, nil
}

func (s *Service) cacheUsageBytes() (int64, error) {
	s.pruneMu.Lock()
	defer s.pruneMu.Unlock()
	return s.cacheUsageBytesLocked()
}

func (s *Service) cacheUsageBytesLocked() (int64, error) {
	_, total, err := s.scanLayerFiles()
	return total, err
}

func (s *Service) scanLayerFiles() ([]cachedLayerFile, int64, error) {
	out := make([]cachedLayerFile, 0, 256)
	var total int64
	err := filepath.WalkDir(s.layersDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".afslyr") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(s.layersDir, path)
		if err != nil {
			return err
		}
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 2 {
			return nil
		}
		algo := strings.TrimSpace(parts[0])
		hexName := strings.TrimSuffix(parts[1], ".afslyr")
		if algo == "" || hexName == "" {
			return nil
		}
		digest := strings.ToLower(algo) + ":" + hexName
		out = append(out, cachedLayerFile{
			digest:  digest,
			path:    path,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		total += info.Size()
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, 0, err
	}
	return out, total, nil
}

func defaultCacheLimitFromFS(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	free := int64(st.Bavail) * int64(st.Bsize)
	if free <= 0 {
		return 0, fmt.Errorf("free space is zero")
	}
	limit := free * defaultCacheRatio / 100
	if limit <= 0 {
		return 0, fmt.Errorf("computed limit is zero")
	}
	return limit, nil
}

func touchFile(path string) error {
	now := time.Now()
	return os.Chtimes(path, now, now)
}

func (s *Service) layerPath(digest string) (string, error) {
	algo, hex, err := splitDigest(digest)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.layersDir, algo, strings.ToLower(hex)+".afslyr"), nil
}

func splitDigest(digest string) (algo, hex string, err error) {
	digest = strings.TrimSpace(digest)
	if !digestPattern.MatchString(digest) {
		return "", "", fmt.Errorf("invalid digest: %q", digest)
	}
	parts := strings.SplitN(digest, ":", 2)
	return strings.ToLower(parts[0]), parts[1], nil
}

func (s *Service) applyConfiguredAuth(f fetcher, registryHost string) error {
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
		return f.LoginWithToken(registryHost, token)
	}
	if username != "" {
		return f.Login(registryHost, username, password)
	}
	return nil
}

func (s *Service) applyConfiguredMirrors(f fetcher) error {
	client, ok := f.(*registry.Client)
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

type digestLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newDigestLocker() *digestLocker {
	return &digestLocker{locks: make(map[string]*sync.Mutex)}
}

func (d *digestLocker) lock(key string) func() {
	d.mu.Lock()
	l, ok := d.locks[key]
	if !ok {
		l = &sync.Mutex{}
		d.locks[key] = l
	}
	d.mu.Unlock()

	l.Lock()
	return l.Unlock
}
