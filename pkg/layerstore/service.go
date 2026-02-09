package layerstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerstorepb"
	"github.com/reyoung/afs/pkg/registry"
)

const (
	defaultPlatformOS   = "linux"
	defaultPlatformArch = "amd64"
	maxReadBytes        = 4 << 20
)

var digestPattern = regexp.MustCompile(`^[A-Za-z0-9_+.-]+:[A-Fa-f0-9]+$`)

type fetcher interface {
	GetLayersForPlatform(ctx context.Context, image, tag, os, arch, variant string) ([]registry.Layer, error)
	DownloadLayer(ctx context.Context, image, tag, digest string) (io.ReadCloser, error)
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

	cacheDir    string
	layersDir   string
	metadataDir string
	locker      *digestLocker
	newFetcher  func() fetcher
	maxReadSize int
	authByHost  map[string]RegistryAuthConfig
	metadataMu  sync.Mutex
	pullReport  func(imageKey string, pulling bool)
}

type imageMetadata struct {
	Image              string           `json:"image,omitempty"`
	Tag                string           `json:"tag,omitempty"`
	PlatformOS         string           `json:"platform_os,omitempty"`
	PlatformArch       string           `json:"platform_arch,omitempty"`
	PlatformVariant    string           `json:"platform_variant,omitempty"`
	ResolvedRegistry   string           `json:"resolved_registry"`
	ResolvedRepository string           `json:"resolved_repository"`
	ResolvedReference  string           `json:"resolved_reference"`
	Layers             []registry.Layer `json:"layers"`
}

func NewService(cacheDir string, authConfigs []RegistryAuthConfig) (*Service, error) {
	if strings.TrimSpace(cacheDir) == "" {
		return nil, fmt.Errorf("cache directory is required")
	}
	layersDir := filepath.Join(cacheDir, "layers")
	if err := os.MkdirAll(layersDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	metadataDir := filepath.Join(cacheDir, "metadata")
	if err := os.MkdirAll(metadataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create metadata dir: %w", err)
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
	return &Service{
		cacheDir:    cacheDir,
		layersDir:   layersDir,
		metadataDir: metadataDir,
		locker:      newDigestLocker(),
		newFetcher:  func() fetcher { return registry.NewClient(nil) },
		maxReadSize: maxReadBytes,
		authByHost:  authByHost,
	}, nil
}

func (s *Service) PullImage(ctx context.Context, req *layerstorepb.PullImageRequest) (*layerstorepb.PullImageResponse, error) {
	if req == nil || strings.TrimSpace(req.GetImage()) == "" {
		return nil, status.Error(codes.InvalidArgument, "image is required")
	}

	ref, err := registry.ParseImageReference(req.GetImage(), req.GetTag())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse image reference: %v", err)
	}

	f := s.newFetcher()
	if err := s.applyConfiguredAuth(f, ref.Registry); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "registry auth config: %v", err)
	}

	platformOS := valueOrDefault(req.GetPlatformOs(), defaultPlatformOS)
	platformArch := valueOrDefault(req.GetPlatformArch(), defaultPlatformArch)
	platformVariant := strings.TrimSpace(req.GetPlatformVariant())
	imgKey := imageKey(req.GetImage(), req.GetTag(), platformOS, platformArch, platformVariant)
	if s.pullReport != nil {
		s.pullReport(imgKey, true)
		defer s.pullReport(imgKey, false)
	}
	log.Printf("pull image requested: image=%s tag=%s platform=%s/%s variant=%s force=%v", req.GetImage(), req.GetTag(), platformOS, platformArch, platformVariant, req.GetForce())

	meta, fromCache, err := s.resolveImageMetadata(ctx, f, req.GetImage(), req.GetTag(), platformOS, platformArch, platformVariant, req.GetForce())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve image metadata: %v", err)
	}
	if fromCache {
		log.Printf("resolved image metadata from cache: registry=%s repository=%s reference=%s layers=%d", meta.ResolvedRegistry, meta.ResolvedRepository, meta.ResolvedReference, len(meta.Layers))
	} else {
		log.Printf("resolved image metadata from registry: registry=%s repository=%s reference=%s layers=%d", meta.ResolvedRegistry, meta.ResolvedRepository, meta.ResolvedReference, len(meta.Layers))
	}

	result := &layerstorepb.PullImageResponse{
		ResolvedRegistry:   meta.ResolvedRegistry,
		ResolvedRepository: meta.ResolvedRepository,
		ResolvedReference:  meta.ResolvedReference,
		Layers:             make([]*layerstorepb.Layer, 0, len(meta.Layers)),
	}

	for i, layer := range meta.Layers {
		log.Printf("[%d/%d] processing layer digest=%s mediaType=%s size=%d", i+1, len(meta.Layers), layer.Digest, layer.MediaType, layer.Size)
		cachePath, afsSize, cached, err := s.ensureLayer(ctx, f, req.GetImage(), req.GetTag(), layer, true)
		if err != nil {
			return nil, err
		}
		if cached {
			log.Printf("[%d/%d] layer cache hit digest=%s afsSize=%d path=%s", i+1, len(meta.Layers), layer.Digest, afsSize, cachePath)
		} else {
			log.Printf("[%d/%d] layer cached digest=%s afsSize=%d path=%s", i+1, len(meta.Layers), layer.Digest, afsSize, cachePath)
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

	log.Printf("pull image completed: image=%s resolved=%s/%s:%s layers=%d", req.GetImage(), result.ResolvedRegistry, result.ResolvedRepository, result.ResolvedReference, len(result.Layers))
	return result, nil
}

func (s *Service) SetPullImageReporter(fn func(imageKey string, pulling bool)) {
	s.pullReport = fn
}

func (s *Service) resolveImageMetadata(ctx context.Context, f fetcher, image, tag, platformOS, platformArch, platformVariant string, force bool) (imageMetadata, bool, error) {
	ref, err := registry.ParseImageReference(image, tag)
	if err != nil {
		return imageMetadata{}, false, err
	}

	cacheKey := metadataCacheKey(ref.Registry, ref.Repository, ref.Reference, platformOS, platformArch, platformVariant)
	if !force {
		if cached, err := s.loadImageMetadata(cacheKey); err == nil {
			return cached, true, nil
		}
	}

	layers, err := f.GetLayersForPlatform(ctx, image, tag, platformOS, platformArch, platformVariant)
	if err != nil {
		return imageMetadata{}, false, err
	}
	meta := imageMetadata{
		Image:              image,
		Tag:                tag,
		PlatformOS:         platformOS,
		PlatformArch:       platformArch,
		PlatformVariant:    platformVariant,
		ResolvedRegistry:   ref.Registry,
		ResolvedRepository: ref.Repository,
		ResolvedReference:  ref.Reference,
		Layers:             append([]registry.Layer(nil), layers...),
	}
	if err := s.saveImageMetadata(cacheKey, meta); err != nil {
		log.Printf("warning: failed to write image metadata cache key=%s: %v", cacheKey, err)
	}
	return meta, false, nil
}

func metadataCacheKey(registryHost, repository, reference, platformOS, platformArch, platformVariant string) string {
	joined := strings.Join([]string{registryHost, repository, reference, platformOS, platformArch, platformVariant}, "|")
	sum := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(sum[:])
}

func (s *Service) metadataPath(cacheKey string) string {
	return filepath.Join(s.metadataDir, cacheKey+".json")
}

func (s *Service) loadImageMetadata(cacheKey string) (imageMetadata, error) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()

	path := s.metadataPath(cacheKey)
	b, err := os.ReadFile(path)
	if err != nil {
		return imageMetadata{}, err
	}
	var meta imageMetadata
	if err := json.Unmarshal(b, &meta); err != nil {
		return imageMetadata{}, err
	}
	if len(meta.Layers) == 0 || meta.ResolvedRegistry == "" || meta.ResolvedRepository == "" || meta.ResolvedReference == "" {
		return imageMetadata{}, fmt.Errorf("invalid metadata cache content: %s", path)
	}
	return meta, nil
}

func (s *Service) saveImageMetadata(cacheKey string, meta imageMetadata) error {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()

	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	path := s.metadataPath(cacheKey)
	tmp, err := os.CreateTemp(s.metadataDir, ".meta-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(b); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
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

	return &layerstorepb.ReadLayerResponse{
		Digest: req.GetDigest(),
		Offset: req.GetOffset(),
		Data:   buf[:n],
		Eof:    errors.Is(readErr, io.EOF),
	}, nil
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

func (s *Service) HasImage(ctx context.Context, req *layerstorepb.HasImageRequest) (*layerstorepb.HasImageResponse, error) {
	_ = ctx
	if req == nil || strings.TrimSpace(req.GetImage()) == "" {
		return nil, status.Error(codes.InvalidArgument, "image is required")
	}
	platformOS := valueOrDefault(req.GetPlatformOs(), defaultPlatformOS)
	platformArch := valueOrDefault(req.GetPlatformArch(), defaultPlatformArch)
	platformVariant := strings.TrimSpace(req.GetPlatformVariant())

	ref, err := registry.ParseImageReference(req.GetImage(), req.GetTag())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse image reference: %v", err)
	}
	cacheKey := metadataCacheKey(ref.Registry, ref.Repository, ref.Reference, platformOS, platformArch, platformVariant)
	meta, err := s.loadImageMetadata(cacheKey)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &layerstorepb.HasImageResponse{Found: false}, nil
		}
		return nil, status.Errorf(codes.Internal, "load image metadata: %v", err)
	}

	missing := 0
	for _, l := range meta.Layers {
		p, err := s.layerPath(l.Digest)
		if err != nil {
			missing++
			continue
		}
		if _, err := os.Stat(p); err != nil {
			missing++
		}
	}
	return &layerstorepb.HasImageResponse{
		Found:              missing == 0,
		ResolvedRegistry:   meta.ResolvedRegistry,
		ResolvedRepository: meta.ResolvedRepository,
		ResolvedReference:  meta.ResolvedReference,
		TotalLayers:        int32(len(meta.Layers)),
		MissingLayers:      int32(missing),
	}, nil
}

func (s *Service) ensureLayer(ctx context.Context, f fetcher, image, tag string, layer registry.Layer, allowDownload bool) (cachePath string, size int64, cached bool, err error) {
	if layer.MediaType != layerformat.OCILayerTarGzipMediaType {
		return "", 0, false, status.Errorf(codes.FailedPrecondition, "unsupported layer media type %q for %s", layer.MediaType, layer.Digest)
	}

	fullPath, err := s.layerPath(layer.Digest)
	if err != nil {
		return "", 0, false, status.Error(codes.InvalidArgument, err.Error())
	}

	if st, statErr := os.Stat(fullPath); statErr == nil {
		return fullPath, st.Size(), true, nil
	}
	if !allowDownload {
		return "", 0, false, status.Errorf(codes.NotFound, "layer is not cached: %s", layer.Digest)
	}

	unlock := s.locker.lock(layer.Digest)
	defer unlock()

	if st, statErr := os.Stat(fullPath); statErr == nil {
		return fullPath, st.Size(), true, nil
	}

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "create layer cache dir: %v", err)
	}

	rc, err := f.DownloadLayer(ctx, image, tag, layer.Digest)
	if err != nil {
		return "", 0, false, status.Errorf(codes.Internal, "download layer %s: %v", layer.Digest, err)
	}
	defer rc.Close()
	log.Printf("downloading layer digest=%s from image=%s tag=%s", layer.Digest, image, tag)

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
	return fullPath, st.Size(), false, nil
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
