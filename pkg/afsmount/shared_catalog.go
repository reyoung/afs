package afsmount

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerfuse"
)

const (
	defaultSharedCatalogMaxImages = 64
	defaultSharedCatalogIdleTTL   = 10 * time.Minute
)

type sharedCatalogManager struct {
	mu        sync.Mutex
	baseDir   string
	mountDir  string
	root      *layerfuse.CatalogRootNode
	server    *fuse.Server
	openless  bool
	images    map[string]*sharedCatalogImage
	layers    map[string]*sharedCatalogLayer
	mountOpts sharedCatalogMountOpts
	maxImages int
	idleTTL   time.Duration
}

type sharedCatalogImage struct {
	name      string
	layers    []string
	refs      int
	lastUsed  time.Time
	createdAt time.Time
}

type sharedCatalogLayer struct {
	info     LayerInfo
	refs     int
	lastUsed time.Time
}

type sharedCatalogMountOpts struct {
	debug     bool
	readAhead int64
}

var (
	sharedCatalogManagersMu sync.Mutex
	sharedCatalogManagers   = map[string]*sharedCatalogManager{}
)

func getSharedCatalogManager(baseDir string) *sharedCatalogManager {
	sharedCatalogManagersMu.Lock()
	defer sharedCatalogManagersMu.Unlock()
	baseDir = filepath.Clean(baseDir)
	mgr := sharedCatalogManagers[baseDir]
	if mgr == nil {
		mgr = &sharedCatalogManager{
			baseDir:   baseDir,
			mountDir:  filepath.Join(baseDir, "fuse-root"),
			images:    make(map[string]*sharedCatalogImage),
			layers:    make(map[string]*sharedCatalogLayer),
			maxImages: defaultSharedCatalogMaxImages,
			idleTTL:   defaultSharedCatalogIdleTTL,
		}
		sharedCatalogManagers[baseDir] = mgr
	}
	return mgr
}

func (m *sharedCatalogManager) ensureMounted(cfg MountConfig) error {
	if m.server != nil {
		return nil
	}
	if err := os.MkdirAll(m.mountDir, 0o755); err != nil {
		return fmt.Errorf("create shared catalog mount dir: %w", err)
	}

	root := layerfuse.NewCatalogRoot()
	entryTimeout := 30 * time.Second
	attrTimeout := 30 * time.Second
	negativeTimeout := 5 * time.Second

	var releaseReaper func()
	if cfg.HoldReaper != nil {
		releaseReaper = cfg.HoldReaper()
	}
	server, err := fusefs.Mount(m.mountDir, root, &fusefs.Options{
		EntryTimeout:    &entryTimeout,
		AttrTimeout:     &attrTimeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			Debug:                cfg.Debug,
			FsName:               "afscatalog",
			Name:                 "afscatalog",
			Options:              []string{"ro", "exec"},
			MaxWrite:             1 << 20,
			MaxReadAhead:         int(cfg.ReadAhead),
			EnableSymlinkCaching: true,
		},
	})
	if releaseReaper != nil {
		releaseReaper()
	}
	if err != nil {
		return fmt.Errorf("mount shared catalog fuse: %w", err)
	}

	m.root = root
	m.server = server
	m.openless = server.KernelSettings().Flags64()&fuse.CAP_NO_OPEN_SUPPORT != 0
	m.mountOpts = sharedCatalogMountOpts{
		debug:     cfg.Debug,
		readAhead: cfg.ReadAhead,
	}
	if cfg.SharedCatalogMaxImages > 0 {
		m.maxImages = cfg.SharedCatalogMaxImages
	}
	if cfg.SharedCatalogIdleTTL > 0 {
		m.idleTTL = cfg.SharedCatalogIdleTTL
	}
	log.Printf("shared catalog mounted at %s openless=%t kernel_flags=%#x", m.mountDir, m.openless, server.KernelSettings().Flags64())
	return nil
}

func (m *sharedCatalogManager) ensureImage(ctx context.Context, imageName string, cfg MountConfig) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validateCatalogName(imageName); err != nil {
		return "", false, err
	}
	if err := m.ensureMounted(cfg); err != nil {
		return "", false, err
	}

	if existing := m.images[imageName]; existing != nil {
		closeLayerRemotes(cfg.Layers)
		existing.refs++
		existing.lastUsed = time.Now()
		return filepath.Join(m.mountDir, existing.name), false, nil
	}

	sharedLayers := make([]LayerInfo, len(cfg.Layers))
	sharedDigests := make([]string, 0, len(cfg.Layers))
	for i, li := range cfg.Layers {
		shared, err := m.acquireLayerLocked(li)
		if err != nil {
			m.releaseLayersLocked(sharedDigests)
			return "", false, err
		}
		sharedLayers[i] = shared
		sharedDigests = append(sharedDigests, shared.Digest)
	}

	overlayLayers := make([]layerfuse.OverlayLayer, len(sharedLayers))
	for i, li := range sharedLayers {
		overlayLayers[i] = layerfuse.OverlayLayer{
			Reader: li.Reader,
			Digest: li.Digest,
		}
	}
	child, _ := layerfuse.NewOverlayRoot(overlayLayers, cfg.PageCache, cfg.ELFCache)
	if m.openless {
		child.SetOpenless(true)
	}
	if err := child.BindSharedELF(ctx, m.root); err != nil {
		m.releaseLayersLocked(sharedDigests)
		return "", false, fmt.Errorf("bind shared ELF for image %s: %w", imageName, err)
	}
	if _, err := m.root.AddImage(ctx, imageName, child); err != nil {
		m.releaseLayersLocked(sharedDigests)
		return "", false, fmt.Errorf("add image %s to shared catalog: %w", imageName, err)
	}

	now := time.Now()
	m.images[imageName] = &sharedCatalogImage{
		name:      imageName,
		layers:    sharedDigests,
		refs:      1,
		lastUsed:  now,
		createdAt: now,
	}
	m.pruneLocked(now)
	return filepath.Join(m.mountDir, imageName), true, nil
}

func (m *sharedCatalogManager) releaseImage(imageName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	img := m.images[imageName]
	if img == nil {
		return
	}
	if img.refs > 0 {
		img.refs--
	}
	img.lastUsed = time.Now()
	m.pruneLocked(img.lastUsed)
}

func (m *sharedCatalogManager) pruneLocked(now time.Time) {
	if len(m.images) == 0 {
		return
	}
	if m.idleTTL > 0 {
		for name, img := range m.images {
			if img.refs != 0 {
				continue
			}
			if now.Sub(img.lastUsed) < m.idleTTL {
				continue
			}
			m.evictLocked(name, "idle_ttl")
		}
	}
	for m.maxImages > 0 && len(m.images) > m.maxImages {
		var victimName string
		var victim *sharedCatalogImage
		for name, img := range m.images {
			if img.refs != 0 {
				continue
			}
			if victim == nil || img.lastUsed.Before(victim.lastUsed) {
				victimName = name
				victim = img
			}
		}
		if victim == nil {
			return
		}
		m.evictLocked(victimName, "max_images")
	}
}

func (m *sharedCatalogManager) evictLocked(name string, reason string) {
	img := m.images[name]
	if img == nil || img.refs != 0 {
		return
	}
	delete(m.images, name)
	m.root.RemoveImage(name)
	m.releaseLayersLocked(img.layers)
	log.Printf("shared catalog evicted image=%s reason=%s idle_for=%s remaining=%d", name, reason, time.Since(img.lastUsed).Round(time.Millisecond), len(m.images))
}

func (m *sharedCatalogManager) acquireLayerLocked(li LayerInfo) (LayerInfo, error) {
	if strings.TrimSpace(li.Digest) == "" {
		return LayerInfo{}, fmt.Errorf("shared catalog layer missing digest")
	}
	if existing := m.layers[li.Digest]; existing != nil {
		existing.refs++
		existing.lastUsed = time.Now()
		if li.Remote != nil && li.Remote != existing.info.Remote {
			_ = li.Remote.Close()
		}
		return existing.info, nil
	}
	if li.Reader == nil || li.Remote == nil {
		return LayerInfo{}, fmt.Errorf("shared catalog layer %s missing reader", li.Digest)
	}
	now := time.Now()
	m.layers[li.Digest] = &sharedCatalogLayer{
		info:     li,
		refs:     1,
		lastUsed: now,
	}
	return li, nil
}

func (m *sharedCatalogManager) releaseLayersLocked(digests []string) {
	now := time.Now()
	for _, digest := range digests {
		layer := m.layers[digest]
		if layer == nil {
			continue
		}
		if layer.refs > 0 {
			layer.refs--
		}
		layer.lastUsed = now
		if layer.refs != 0 {
			continue
		}
		delete(m.layers, digest)
		if layer.info.Remote != nil {
			_ = layer.info.Remote.Close()
		}
	}
}

func closeLayerRemotes(layers []LayerInfo) {
	for _, li := range layers {
		if li.Remote != nil {
			_ = li.Remote.Close()
		}
	}
}

func imageCatalogName(image string, tag string, layers []LayerInfo, platformOS, platformArch, platformVariant string) string {
	var b strings.Builder
	b.WriteString(image)
	b.WriteString("|")
	b.WriteString(tag)
	b.WriteString("|")
	b.WriteString(platformOS)
	b.WriteString("|")
	b.WriteString(platformArch)
	b.WriteString("|")
	b.WriteString(platformVariant)
	for _, li := range layers {
		b.WriteString("|")
		b.WriteString(li.Digest)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "img-" + hex.EncodeToString(sum[:8])
}

func defaultSharedMountRoot(workDir string) string {
	parent := filepath.Dir(workDir)
	if filepath.Base(parent) == "work" {
		parent = filepath.Dir(parent)
	}
	if parent == "." || parent == "/" || parent == "" {
		return filepath.Join(os.TempDir(), "afs-shared")
	}
	return filepath.Join(parent, "shared-catalog")
}

func validateCatalogName(name string) error {
	if name == "" || strings.Contains(name, "/") || strings.Contains(name, string(os.PathSeparator)) {
		return syscall.EINVAL
	}
	return nil
}
