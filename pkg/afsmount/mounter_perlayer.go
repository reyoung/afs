package afsmount

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerfuse"
	"github.com/reyoung/afs/pkg/layerreader"
)

// PerLayerMounter mounts each OCI layer as a separate FUSE filesystem.
type PerLayerMounter struct {
	Concurrency int // layer mount concurrency, default 1
}

type perLayerMounted struct {
	digest    string
	dir       string
	server    *fuse.Server
	reader    *layerreader.DiscoveryBackedReaderAt
	fuseStats *layerfuse.FuseStats
}

func (m *PerLayerMounter) Mount(ctx context.Context, cfg MountConfig) (*MountResult, error) {
	concurrency := m.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	layersRoot := filepath.Join(cfg.WorkDir, "layers")
	if err := os.MkdirAll(layersRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create layer work dir: %w", err)
	}

	layers := cfg.Layers
	results := make([]perLayerMounted, len(layers))
	type mountResult struct {
		idx   int
		layer perLayerMounted
		err   error
	}
	resultCh := make(chan mountResult, len(layers))
	sema := make(chan struct{}, effectiveLayerMountConcurrency(concurrency, len(layers)))
	var stop atomic.Bool
	var wg sync.WaitGroup
	layersPrepareStarted := time.Now()

	for i, layer := range layers {
		i := i
		layer := layer
		wg.Add(1)
		go func() {
			defer wg.Done()
			sema <- struct{}{}
			defer func() { <-sema }()
			if stop.Load() {
				return
			}
			digest := layer.Digest
			layerFields := []string{
				"index=" + strconv.Itoa(i+1),
				"digest=" + digest,
			}
			layerStarted := time.Now()
			log.Printf("[%d/%d] preparing layer digest=%s", i+1, len(layers), digest)
			mountDir := filepath.Join(layersRoot, fmt.Sprintf("%03d_%s", i+1, sanitizeForPath(shortDigest(digest))))
			if err := os.MkdirAll(mountDir, 0o755); err != nil {
				logTiming("layer_prepare_mkdir_dir", layerStarted, append(layerFields, "ok=false")...)
				stop.Store(true)
				resultCh <- mountResult{idx: i, err: fmt.Errorf("create layer mount dir: %w", err)}
				return
			}

			layerFuseMountStarted := time.Now()
			server, layerFuseStats, err := mountLayerReader(layer.Reader, mountDir, "discovery:"+digest, digest, cfg.Debug, cfg.ReadAhead, cfg.PageCache, cfg.HoldReaper)
			if err != nil {
				logTiming("layer_prepare_fuse_mount", layerFuseMountStarted, append(layerFields, "ok=false")...)
				stop.Store(true)
				resultCh <- mountResult{idx: i, err: fmt.Errorf("mount layer %s: %w", digest, err)}
				return
			}
			logTiming("layer_prepare_fuse_mount", layerFuseMountStarted, append(layerFields, "ok=true")...)
			logTiming("layer_prepare_total", layerStarted, append(layerFields, "ok=true")...)
			resultCh <- mountResult{
				idx: i,
				layer: perLayerMounted{
					digest:    digest,
					dir:       mountDir,
					server:    server,
					reader:    layer.Remote,
					fuseStats: layerFuseStats,
				},
			}
		}()
	}
	wg.Wait()
	logTiming("layers_prepare_parallel_total", layersPrepareStarted, "layers="+strconv.Itoa(len(layers)))
	close(resultCh)

	var mountErr error
	for r := range resultCh {
		if r.err != nil {
			if mountErr == nil {
				mountErr = r.err
			}
			continue
		}
		results[r.idx] = r.layer
	}

	// On error, clean up any successfully mounted layers.
	if mountErr != nil {
		for _, ml := range results {
			if ml.server != nil {
				_ = ml.server.Unmount()
				ml.server.Wait()
			}
		}
		return nil, mountErr
	}

	mounted := make([]perLayerMounted, 0, len(results))
	layerDirs := make([]string, 0, len(results))
	stats := make([]*layerfuse.FuseStats, 0, len(results))
	for i, ml := range results {
		if ml.server == nil {
			// Clean up anything mounted so far.
			for _, m := range mounted {
				_ = m.server.Unmount()
				m.server.Wait()
			}
			return nil, fmt.Errorf("layer %d mount result missing", i)
		}
		mounted = append(mounted, ml)
		layerDirs = append(layerDirs, ml.dir)
		stats = append(stats, ml.fuseStats)
		log.Printf("[%d/%d] mounted layer %s at %s", i+1, len(results), ml.digest, ml.dir)
	}

	cleanup := func() {
		for i := len(mounted) - 1; i >= 0; i-- {
			if mounted[i].fuseStats != nil {
				mounted[i].fuseStats.Log(fmt.Sprintf("layer[%d:%s]", i, mounted[i].digest[:min(16, len(mounted[i].digest))]))
			}
			_ = mounted[i].server.Unmount()
			mounted[i].server.Wait()
			if mounted[i].reader != nil {
				_ = mounted[i].reader.Close()
			}
		}
	}

	return &MountResult{
		LayerDirs: layerDirs,
		Cleanup:   cleanup,
		Stats:     stats,
	}, nil
}
