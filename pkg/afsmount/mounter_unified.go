package afsmount

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerfuse"
)

// UnifiedMounter mounts all OCI layers as a single unified FUSE filesystem.
type UnifiedMounter struct{}

func (m *UnifiedMounter) Mount(ctx context.Context, cfg MountConfig) (*MountResult, error) {
	mountDir := filepath.Join(cfg.WorkDir, "layers", "unified")
	if err := os.MkdirAll(mountDir, 0o755); err != nil {
		return nil, fmt.Errorf("create unified mount dir: %w", err)
	}

	// Build overlay layers (bottom-to-top order, same as cfg.Layers).
	overlayLayers := make([]layerfuse.OverlayLayer, len(cfg.Layers))
	for i, li := range cfg.Layers {
		overlayLayers[i] = layerfuse.OverlayLayer{
			Reader: li.Reader,
			Digest: li.Digest,
		}
	}

	buildStarted := time.Now()
	root, fuseStats := layerfuse.NewOverlayRoot(overlayLayers, cfg.PageCache)
	logTiming("unified_overlay_build", buildStarted, "layers="+fmt.Sprintf("%d", len(cfg.Layers)))

	entryTimeout := 30 * time.Second
	attrTimeout := 30 * time.Second
	negativeTimeout := 5 * time.Second

	var releaseReaper func()
	if cfg.HoldReaper != nil {
		releaseReaper = cfg.HoldReaper()
	}
	mountStarted := time.Now()
	server, err := fusefs.Mount(mountDir, root, &fusefs.Options{
		EntryTimeout:    &entryTimeout,
		AttrTimeout:     &attrTimeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			Debug:        cfg.Debug,
			FsName:       "afsunified",
			Name:         "afsunified",
			Options:      []string{"ro", "exec"},
			MaxWrite:     1 << 20,
			MaxReadAhead: int(cfg.ReadAhead),
		},
	})
	if releaseReaper != nil {
		releaseReaper()
	}
	if err != nil {
		if strings.Contains(err.Error(), "no FUSE mount utility found") {
			return nil, fmt.Errorf("mount unified fuse: %w (hint: install FUSE runtime)", err)
		}
		return nil, fmt.Errorf("mount unified fuse: %w", err)
	}
	logTiming("unified_fuse_mount", mountStarted)
	log.Printf("unified overlay mounted at %s with %d layers", mountDir, len(cfg.Layers))

	cleanup := func() {
		fuseStats.Log("unified")
		_ = server.Unmount()
		server.Wait()
		for _, li := range cfg.Layers {
			if li.Remote != nil {
				_ = li.Remote.Close()
			}
		}
	}

	return &MountResult{
		LayerDirs: []string{mountDir},
		Cleanup:   cleanup,
		Stats:     []*layerfuse.FuseStats{fuseStats},
	}, nil
}

// UnifiedRWMounter mounts all OCI layers as a single read-write FUSE filesystem.
// Writes go to an upper directory on local disk; reads fall through to the merged layers.
// This eliminates the need for fuse-overlayfs as a separate process.
type UnifiedRWMounter struct{}

func (m *UnifiedRWMounter) Mount(ctx context.Context, cfg MountConfig) (*MountResult, error) {
	if cfg.Mountpoint == "" {
		return nil, fmt.Errorf("unified-rw mode requires Mountpoint in MountConfig")
	}

	// Create writable upper dir.
	upperDir := cfg.WritableDir
	if upperDir == "" {
		upperDir = filepath.Join(cfg.WorkDir, "writable-upper")
	}
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		return nil, fmt.Errorf("create writable upper dir: %w", err)
	}

	// Build overlay layers (bottom-to-top order, same as cfg.Layers).
	overlayLayers := make([]layerfuse.OverlayLayer, len(cfg.Layers))
	for i, li := range cfg.Layers {
		overlayLayers[i] = layerfuse.OverlayLayer{
			Reader: li.Reader,
			Digest: li.Digest,
		}
	}

	buildStarted := time.Now()
	root, fuseStats := layerfuse.NewOverlayRootRW(overlayLayers, cfg.PageCache, upperDir, cfg.ExtraDir)
	logTiming("unified_rw_overlay_build", buildStarted, "layers="+fmt.Sprintf("%d", len(cfg.Layers)))

	// Use shorter timeouts for RW mode since the kernel may invalidate
	// cached entries when writes occur.
	entryTimeout := 30 * time.Second
	attrTimeout := 30 * time.Second
	negativeTimeout := 5 * time.Second

	var releaseReaper func()
	if cfg.HoldReaper != nil {
		releaseReaper = cfg.HoldReaper()
	}
	mountStarted := time.Now()
	server, err := fusefs.Mount(cfg.Mountpoint, root, &fusefs.Options{
		EntryTimeout:    &entryTimeout,
		AttrTimeout:     &attrTimeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			Debug:        cfg.Debug,
			FsName:       "afsunifiedrw",
			Name:         "afsunifiedrw",
			Options:      []string{"exec"},
			AllowOther:   true,
			MaxWrite:     1 << 20,
			MaxReadAhead: int(cfg.ReadAhead),
		},
	})
	if releaseReaper != nil {
		releaseReaper()
	}
	if err != nil {
		if strings.Contains(err.Error(), "no FUSE mount utility found") {
			return nil, fmt.Errorf("mount unified-rw fuse: %w (hint: install FUSE runtime)", err)
		}
		return nil, fmt.Errorf("mount unified-rw fuse: %w", err)
	}
	logTiming("unified_rw_fuse_mount", mountStarted)
	log.Printf("unified-rw overlay mounted at %s with %d layers (upper=%s)", cfg.Mountpoint, len(cfg.Layers), upperDir)

	cleanup := func() {
		fuseStats.Log("unified-rw")
		_ = server.Unmount()
		server.Wait()
		for _, li := range cfg.Layers {
			if li.Remote != nil {
				_ = li.Remote.Close()
			}
		}
	}

	return &MountResult{
		LayerDirs:   []string{cfg.Mountpoint},
		Cleanup:     cleanup,
		Stats:       []*layerfuse.FuseStats{fuseStats},
		DirectMount: true,
	}, nil
}
