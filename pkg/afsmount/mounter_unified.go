package afsmount

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// UnifiedMounter mounts all OCI layers as a single unified FUSE filesystem.
type UnifiedMounter struct{}

func (m *UnifiedMounter) Mount(ctx context.Context, cfg MountConfig) (*MountResult, error) {
	if cfg.ImageName == "" {
		return nil, fmt.Errorf("missing image name for shared catalog mount")
	}
	if err := os.MkdirAll(filepath.Join(cfg.WorkDir, "layers"), 0o755); err != nil {
		return nil, fmt.Errorf("create layers dir: %w", err)
	}

	sharedRoot := cfg.SharedRoot
	if sharedRoot == "" {
		sharedRoot = defaultSharedMountRoot(cfg.WorkDir)
	}
	manager := getSharedCatalogManager(sharedRoot)
	buildStarted := time.Now()
	layerDir, created, err := manager.ensureImage(ctx, cfg.ImageName, cfg)
	logTiming("unified_overlay_build", buildStarted, "layers="+fmt.Sprintf("%d", len(cfg.Layers)), "created="+fmt.Sprintf("%t", created))
	if err != nil {
		if strings.Contains(err.Error(), "no FUSE mount utility found") {
			return nil, fmt.Errorf("mount unified fuse: %w (hint: install FUSE runtime)", err)
		}
		return nil, err
	}
	log.Printf("unified overlay mounted at %s with %d layers (shared_root=%s image=%s created=%t)", layerDir, len(cfg.Layers), sharedRoot, cfg.ImageName, created)

	cleanup := func() {
		manager.releaseImage(cfg.ImageName)
	}

	return &MountResult{
		LayerDirs: []string{layerDir},
		Cleanup:   cleanup,
		Stats:     nil,
	}, nil
}
