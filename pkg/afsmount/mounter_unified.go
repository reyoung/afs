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
			Debug:                cfg.Debug,
			FsName:               "afsunified",
			Name:                 "afsunified",
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
