package afsmount

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/debughttp"
	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/grpcclientcache"
	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerfuse"
	"github.com/reyoung/afs/pkg/layerreader"
	"github.com/reyoung/afs/pkg/layerstorepb"
	"github.com/reyoung/afs/pkg/pagecache"
)

const (
	DefaultGRPCTimeout          = 30 * time.Second
	DefaultFUSEMaxReadAhead     = 8 << 20
	maxCachedGRPCConns          = 128
	imageResolveCacheTTL        = 45 * time.Second
	imageResolveCacheMaxEntries = 256
	layerSvcCacheTTL            = 5 * time.Second
	layerSvcCacheMaxEntries     = 2048
	unionMountReadyTimeout      = 120 * time.Second
	unionMountReadyPollInterval = 1 * time.Millisecond
)

type Config struct {
	Mountpoint            string
	Debug                 bool
	MountProcDev          bool
	ExtraDir              string
	DiscoveryAddr         string
	GRPCTimeout           time.Duration
	GRPCMaxChunk          int
	FUSEMaxReadAheadBytes int64
	GRPCInsecure          bool
	NodeID                string
	Image                 string
	Tag                   string
	PlatformOS            string
	PlatformArch          string
	PlatformVariant       string
	ForceLocalFetch       bool
	PullTimeout           time.Duration
	WorkDir               string
	KeepWorkDir           bool
	LayerMountConcurrency int
	PprofListen           string
	OnReady               func()
	PageCacheStore        *pagecache.Store
	HoldReaper            func() func()
	TOCCache              *layerformat.TOCCache
	MountMode             string // "unified-rw" (default), "per-layer", or "unified" (default), "unified", or "unified-rw"
}

type config struct {
	mountpoint            string
	debug                 bool
	mountProcDev          bool
	extraDir              string
	discoveryAddr         string
	grpcTimeout           time.Duration
	grpcMaxChunk          int
	fuseMaxReadAheadBytes int64
	grpcInsecure          bool
	nodeID                string
	image                 string
	tag                   string
	platformOS            string
	platformArch          string
	platformVariant       string
	forceLocalFetch       bool
	pullTimeout           time.Duration
	workDir               string
	keepWorkDir           bool
	layerMountConcurrency int
	pprofListen           string
	onReady               func()
	pageCacheStore        *pagecache.Store
	holdReaper            func() func()
	tocCache              *layerformat.TOCCache
	mountMode             string
}

type serviceInfo struct {
	nodeID   string
	endpoint string
}

type imageResolveCacheEntry struct {
	expireAt  time.Time
	chosen    serviceInfo
	pullResp  *layerstorepb.PullImageResponse
	providers []serviceInfo
}

type imageResolveCache struct {
	mu      sync.Mutex
	entries map[string]imageResolveCacheEntry
}

type layerServiceCacheEntry struct {
	expireAt time.Time
	services []serviceInfo
}

type layerServiceCache struct {
	mu      sync.Mutex
	entries map[string]layerServiceCacheEntry
}

var (
	sharedGRPCConnCache = grpcclientcache.New(maxCachedGRPCConns)
	sharedImageCache    = &imageResolveCache{entries: make(map[string]imageResolveCacheEntry)}
	sharedLayerSvcCache = &layerServiceCache{entries: make(map[string]layerServiceCacheEntry)}
)

func logTiming(phase string, startedAt time.Time, fields ...string) {
	extra := ""
	if len(fields) > 0 {
		extra = " " + strings.Join(fields, " ")
	}
	log.Printf("__AFS_MOUNT_TIMING__ phase=%s ms=%d%s", phase, time.Since(startedAt).Milliseconds(), extra)
}

func safeInvokeReadyCallback(onReady func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("mount ready callback panic: %v", r)
		}
	}()
	onReady()
}

func withTimeoutFromParent(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func Run(ctx context.Context, userCfg Config) error {
	cfg, err := normalizeConfig(userCfg)
	if err != nil {
		return err
	}
	shutdownPprof := debughttp.StartPprofServer("afs_mount", cfg.pprofListen)
	if shutdownPprof != nil {
		defer func() {
			pprofCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownPprof(pprofCtx)
		}()
	}

	log.Printf("starting mount client: image=%s tag=%s mountpoint=%s discovery=%s node-id=%s platform=%s/%s variant=%s force_local_fetch=%v", cfg.image, cfg.tag, cfg.mountpoint, cfg.discoveryAddr, cfg.nodeID, cfg.platformOS, cfg.platformArch, cfg.platformVariant, cfg.forceLocalFetch)

	if err := ensureMountpoint(cfg.mountpoint); err != nil {
		return fmt.Errorf("invalid mountpoint: %w", err)
	}

	discoveryConn, releaseDiscoveryConn, err := sharedGRPCConnCache.Acquire(cfg.discoveryAddr, cfg.grpcTimeout, cfg.grpcInsecure)
	if err != nil {
		return fmt.Errorf("dial discovery gRPC %s: %w", cfg.discoveryAddr, err)
	}
	defer releaseDiscoveryConn()

	discoveryClient := discoverypb.NewServiceDiscoveryClient(discoveryConn)
	return runImageMode(ctx, discoveryClient, cfg)
}

func normalizeConfig(userCfg Config) (config, error) {
	cfg := config{
		mountpoint:            strings.TrimSpace(userCfg.Mountpoint),
		debug:                 userCfg.Debug,
		mountProcDev:          userCfg.MountProcDev,
		extraDir:              strings.TrimSpace(userCfg.ExtraDir),
		discoveryAddr:         strings.TrimSpace(userCfg.DiscoveryAddr),
		grpcTimeout:           userCfg.GRPCTimeout,
		grpcMaxChunk:          userCfg.GRPCMaxChunk,
		fuseMaxReadAheadBytes: userCfg.FUSEMaxReadAheadBytes,
		grpcInsecure:          userCfg.GRPCInsecure,
		nodeID:                strings.TrimSpace(userCfg.NodeID),
		image:                 strings.TrimSpace(userCfg.Image),
		tag:                   strings.TrimSpace(userCfg.Tag),
		platformOS:            strings.TrimSpace(userCfg.PlatformOS),
		platformArch:          strings.TrimSpace(userCfg.PlatformArch),
		platformVariant:       strings.TrimSpace(userCfg.PlatformVariant),
		forceLocalFetch:       userCfg.ForceLocalFetch,
		pullTimeout:           userCfg.PullTimeout,
		workDir:               strings.TrimSpace(userCfg.WorkDir),
		keepWorkDir:           userCfg.KeepWorkDir,
		layerMountConcurrency: userCfg.LayerMountConcurrency,
		pprofListen:           strings.TrimSpace(userCfg.PprofListen),
		onReady:               userCfg.OnReady,
		pageCacheStore:        userCfg.PageCacheStore,
		holdReaper:            userCfg.HoldReaper,
		tocCache:              userCfg.TOCCache,
		mountMode:             strings.TrimSpace(userCfg.MountMode),
	}

	if cfg.discoveryAddr == "" {
		cfg.discoveryAddr = "127.0.0.1:60051"
	}
	if cfg.grpcTimeout <= 0 {
		cfg.grpcTimeout = 10 * time.Second
	}
	if cfg.grpcMaxChunk <= 0 {
		cfg.grpcMaxChunk = 4 << 20
	}
	if cfg.fuseMaxReadAheadBytes <= 0 {
		cfg.fuseMaxReadAheadBytes = DefaultFUSEMaxReadAhead
	}
	if cfg.platformOS == "" {
		cfg.platformOS = "linux"
	}
	if cfg.platformArch == "" {
		cfg.platformArch = "amd64"
	}
	if cfg.pullTimeout <= 0 {
		cfg.pullTimeout = 20 * time.Minute
	}
	if cfg.layerMountConcurrency <= 0 {
		cfg.layerMountConcurrency = 1
	}
	if !cfg.grpcInsecure {
		cfg.grpcInsecure = true
	}
	if cfg.mountMode == "" {
		cfg.mountMode = "unified-rw"
	}

	if cfg.mountpoint == "" {
		return config{}, fmt.Errorf("-mountpoint is required")
	}
	if cfg.image == "" {
		return config{}, fmt.Errorf("-image is required")
	}
	if strings.TrimSpace(cfg.extraDir) != "" {
		if err := ensureMountpoint(cfg.extraDir); err != nil {
			return config{}, fmt.Errorf("invalid -extra-dir: %w", err)
		}
	}
	return cfg, nil
}

func runImageMode(ctx context.Context, discoveryClient discoverypb.ServiceDiscoveryClient, cfg config) error {
	runStarted := time.Now()
	defer logTiming(
		"run_image_mode_total",
		runStarted,
		"image="+cfg.image,
		"tag="+cfg.tag,
		"concurrency="+strconv.Itoa(cfg.layerMountConcurrency),
	)

	var (
		chosen    serviceInfo
		pullResp  *layerstorepb.PullImageResponse
		providers []serviceInfo
		err       error
	)
	imageKeyStr := imageKey(cfg.image, cfg.tag, cfg.platformOS, cfg.platformArch, cfg.platformVariant)
	cacheKey := imageResolveCacheKey(cfg, imageKeyStr)
	cacheLookupStarted := time.Now()
	if cached, ok := sharedImageCache.get(cacheKey); ok {
		chosen = cached.chosen
		pullResp = cached.pullResp
		providers = append([]serviceInfo(nil), cached.providers...)
		log.Printf("image resolve cache hit: image=%s tag=%s layers=%d provider=%s", cfg.image, cfg.tag, len(pullResp.GetLayers()), chosen.endpoint)
		logTiming("image_resolve_cache_lookup", cacheLookupStarted, "hit=true")
	} else {
		logTiming("image_resolve_cache_lookup", cacheLookupStarted, "hit=false")

		findImageByKeyStarted := time.Now()
		imageProviders, findErr := findImageServices(ctx, discoveryClient, imageKeyStr, cfg.grpcTimeout)
		if findErr != nil {
			logTiming("discovery_find_image_by_key", findImageByKeyStarted, "providers=0", "ok=false")
			return findErr
		}
		logTiming("discovery_find_image_by_key", findImageByKeyStarted, "providers="+strconv.Itoa(len(imageProviders)), "ok=true")
		log.Printf("discovery returned %d providers with image", len(imageProviders))

		allServices := imageProviders
		if len(allServices) == 0 {
			findAllServicesStarted := time.Now()
			allServices, err = listDiscoveryProviders(ctx, discoveryClient, cfg.grpcTimeout)
			if err != nil {
				logTiming("discovery_find_all_services", findAllServicesStarted, "providers=0", "ok=false")
				return err
			}
			logTiming("discovery_find_all_services", findAllServicesStarted, "providers="+strconv.Itoa(len(allServices)), "ok=true")
			if len(allServices) == 0 {
				return fmt.Errorf("no layerstore services registered in discovery")
			}
			log.Printf("no cached image providers found; fallback to %d total services", len(allServices))
		}

		pullWithDiscoveryStarted := time.Now()
		chosen, pullResp, err = pullImageWithDiscovery(ctx, imageProviders, allServices, cfg)
		if err != nil {
			logTiming("pull_image_with_discovery", pullWithDiscoveryStarted, "layers=0", "ok=false")
			return err
		}
		logTiming("pull_image_with_discovery", pullWithDiscoveryStarted, "layers="+strconv.Itoa(len(pullResp.GetLayers())), "ok=true")
		log.Printf("image resolved from endpoint=%s resolved=%s/%s:%s layers=%d", chosen.endpoint, pullResp.GetResolvedRegistry(), pullResp.GetResolvedRepository(), pullResp.GetResolvedReference(), len(pullResp.GetLayers()))

		discoverProvidersStarted := time.Now()
		providers, err = discoverImageProviders(allServices, chosen, cfg)
		logTiming("discover_image_providers", discoverProvidersStarted, "providers="+strconv.Itoa(len(providers)))
		if err != nil {
			return err
		}
		sharedImageCache.put(cacheKey, imageResolveCacheEntry{
			expireAt:  time.Now().Add(imageResolveCacheTTL),
			chosen:    chosen,
			pullResp:  pullResp,
			providers: append([]serviceInfo(nil), providers...),
		})
	}
	if len(providers) == 0 && chosen.endpoint != "" {
		providers = []serviceInfo{chosen}
	}
	log.Printf("image providers available for failover: %d", len(providers))

	resolveWorkDirStarted := time.Now()
	workDir, autoCreated, err := resolveWorkDir(cfg.workDir)
	logTiming("resolve_work_dir", resolveWorkDirStarted, "auto_created="+fmt.Sprintf("%t", autoCreated))
	if err != nil {
		return err
	}
	if autoCreated {
		log.Printf("using temp work directory: %s", workDir)
	} else {
		log.Printf("using work directory: %s", workDir)
	}
	cleanupWorkDir := func() {}
	if autoCreated && !cfg.keepWorkDir {
		cleanupWorkDir = func() { _ = os.RemoveAll(workDir) }
	}
	defer cleanupWorkDir()

	var cacheStore *pagecache.Store
	if cfg.pageCacheStore != nil {
		cacheStore = cfg.pageCacheStore
		log.Printf("page cache store attached (in-process)")
	}

	// Prepare layer readers from pullResp.
	layers := pullResp.GetLayers()
	layerInfos := make([]LayerInfo, len(layers))
	layerReadersStarted := time.Now()
	for i, layer := range layers {
		digest := layer.GetDigest()
		layerFields := []string{
			"index=" + strconv.Itoa(i+1),
			"digest=" + digest,
		}

		readerInitStarted := time.Now()
		reader, err := layerreader.NewDiscoveryBackedLayerReader(
			layerreader.Config{
				DiscoveryAddr:     cfg.discoveryAddr,
				GRPCTimeout:       cfg.grpcTimeout,
				GRPCMaxChunk:      cfg.grpcMaxChunk,
				GRPCInsecure:      cfg.grpcInsecure,
				NodeID:            cfg.nodeID,
				KnownLayerAfsSize: layer.GetAfsSize(),
				DialGRPCAcquire: func(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, func(), error) {
					return sharedGRPCConnCache.Acquire(addr, timeout, insecureTransport)
				},
				FindLayerServices: func(discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]layerreader.ServiceInfo, error) {
					services, err := findLayerServicesCached(ctx, discoveryAddr, digest, timeout, insecureTransport)
					if err != nil {
						return nil, err
					}
					return toLayerReaderServices(services), nil
				},
			},
			digest,
			chosen.endpoint,
			toLayerReaderServices(providers),
		)
		if err != nil {
			logTiming("layer_prepare_reader_init", readerInitStarted, append(layerFields, "ok=false")...)
			// Close any already-opened readers.
			for j := 0; j < i; j++ {
				if layerInfos[j].Remote != nil {
					_ = layerInfos[j].Remote.Close()
				}
			}
			return fmt.Errorf("prepare discovery-backed layer reader for %s: %w", digest, err)
		}
		logTiming("layer_prepare_reader_init", readerInitStarted, append(layerFields, "ok=true")...)

		layerformatOpenStarted := time.Now()
		observedReader := layerreader.NewObservedReaderAt(reader, layerreader.ObserveConfig{
			Name: "discovery:" + digest,
		})
		afslReader, err := layerformat.NewReaderCached(observedReader, cfg.tocCache, digest)
		if err != nil {
			logTiming("layer_prepare_open_layerformat", layerformatOpenStarted, append(layerFields, "ok=false")...)
			_ = reader.Close()
			for j := 0; j < i; j++ {
				if layerInfos[j].Remote != nil {
					_ = layerInfos[j].Remote.Close()
				}
			}
			return fmt.Errorf("open layer %s: %w", digest, err)
		}
		logTiming("layer_prepare_open_layerformat", layerformatOpenStarted, append(layerFields, "ok=true")...)

		layerInfos[i] = LayerInfo{
			Digest: digest,
			Reader: afslReader,
			Remote: reader,
		}
	}
	logTiming("layer_readers_prepare_total", layerReadersStarted, "layers="+strconv.Itoa(len(layers)))

	// Choose mounter based on config.
	var mounter Mounter
	switch cfg.mountMode {
	case "unified":
		mounter = &UnifiedMounter{}
	case "unified-rw":
		mounter = &UnifiedRWMounter{}
	default:
		mounter = &PerLayerMounter{Concurrency: cfg.layerMountConcurrency}
	}

	mountCfg := MountConfig{
		Layers:      layerInfos,
		WorkDir:     workDir,
		Debug:       cfg.debug,
		ReadAhead:   cfg.fuseMaxReadAheadBytes,
		PageCache:   cacheStore,
		TOCCache:    cfg.tocCache,
		HoldReaper:  cfg.holdReaper,
		Mountpoint:  cfg.mountpoint,
		ExtraDir:    cfg.extraDir,
		WritableDir: filepath.Join(workDir, "writable-upper"),
	}
	mountResult, err := mounter.Mount(ctx, mountCfg)
	if err != nil {
		// Close readers on mount failure.
		for _, li := range layerInfos {
			if li.Remote != nil {
				_ = li.Remote.Close()
			}
		}
		return err
	}
	defer mountResult.Cleanup()

	// DirectMount: the mounter already mounted at cfg.mountpoint with write support.
	// Skip fuse-overlayfs entirely; just mount extra filesystems and wait.
	if mountResult.DirectMount {
		mountExtraFSStarted := time.Now()
		extraMountTargets, err := mountExtraFilesystems(runtime.GOOS, cfg.mountpoint, cfg.mountProcDev)
		if err != nil {
			return err
		}
		logTiming("mount_extra_filesystems", mountExtraFSStarted, "targets="+strconv.Itoa(len(extraMountTargets)))
		defer func() {
			if err := unmountExtraFilesystems(runtime.GOOS, extraMountTargets); err != nil {
				log.Printf("unmount extra filesystems failed: %v", err)
			}
		}()

		if cfg.onReady != nil {
			safeInvokeReadyCallback(cfg.onReady)
		}

		// Block until context is cancelled.
		<-ctx.Done()
		return nil
	}

	layerDirs := mountResult.LayerDirs

	var writableLayerDir string
	var writableWorkDir string
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		writableLayerDir = filepath.Join(workDir, "writable-upper")
		mkdirWritableUpperStarted := time.Now()
		if err := os.MkdirAll(writableLayerDir, 0o755); err != nil {
			return fmt.Errorf("create writable upper dir: %w", err)
		}
		logTiming("prepare_writable_upper_dir", mkdirWritableUpperStarted)
	}
	if runtime.GOOS == "linux" {
		writableWorkDir = filepath.Join(workDir, "writable-work")
		mkdirWritableWorkStarted := time.Now()
		if err := os.MkdirAll(writableWorkDir, 0o755); err != nil {
			return fmt.Errorf("create writable work dir: %w", err)
		}
		logTiming("prepare_writable_work_dir", mkdirWritableWorkStarted)
	}

	buildUnionCmdStarted := time.Now()
	unionCmd, err := buildUnionMountCommand(runtime.GOOS, layerDirs, cfg.mountpoint, writableLayerDir, writableWorkDir, cfg.extraDir)
	if err != nil {
		return err
	}
	logTiming("build_union_mount_command", buildUnionCmdStarted)
	log.Printf("starting union mount command: %s %s", unionCmd.Path, strings.Join(unionCmd.Args[1:], " "))
	unionCmd.Stdout = os.Stdout
	unionCmd.Stderr = os.Stderr
	startUnionCmdStarted := time.Now()
	if err := unionCmd.Start(); err != nil {
		return fmt.Errorf("start union mount: %w", err)
	}
	logTiming("start_union_mount_process", startUnionCmdStarted)
	mountExtraFSStarted := time.Now()
	extraMountTargets, err := mountExtraFilesystems(runtime.GOOS, cfg.mountpoint, cfg.mountProcDev)
	if err != nil {
		_ = unionCmd.Process.Signal(syscall.SIGTERM)
		_ = tryUnmountUnion(runtime.GOOS, cfg.mountpoint)
		return err
	}
	logTiming("mount_extra_filesystems", mountExtraFSStarted, "targets="+strconv.Itoa(len(extraMountTargets)))
	defer func() {
		if err := unmountExtraFilesystems(runtime.GOOS, extraMountTargets); err != nil {
			log.Printf("unmount extra filesystems failed: %v", err)
		}
	}()

	waitCh := make(chan error, 1)
	go func() { waitCh <- unionCmd.Wait() }()
	stopUnion := func(reason string) {
		stopUnionStarted := time.Now()
		log.Printf("stopping union mount (%s)", reason)
		unmountExtraFSStarted := time.Now()
		if err := unmountExtraFilesystems(runtime.GOOS, extraMountTargets); err != nil {
			log.Printf("explicit extra unmount failed: %v", err)
		}
		logTiming("stop_unmount_extra_filesystems", unmountExtraFSStarted, "targets="+strconv.Itoa(len(extraMountTargets)))
		unmountUnionStarted := time.Now()
		if err := tryUnmountUnion(runtime.GOOS, cfg.mountpoint); err != nil {
			log.Printf("explicit union unmount failed: %v", err)
		} else {
			log.Printf("union mount unmounted: %s", cfg.mountpoint)
		}
		logTiming("stop_unmount_union", unmountUnionStarted)
		_ = unionCmd.Process.Signal(syscall.SIGTERM)
		waitUnionExitStarted := time.Now()
		waitForExit := func(timeout time.Duration) (error, bool) {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case err := <-waitCh:
				return err, true
			case <-timer.C:
				return nil, false
			}
		}
		if err, exited := waitForExit(5 * time.Second); exited {
			if err != nil {
				log.Printf("union mount exited with error: %v", err)
			}
		} else {
			if killErr := unionCmd.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				log.Printf("force kill union mount failed: %v", killErr)
			}
			if err, killedExited := waitForExit(2 * time.Second); killedExited {
				if err != nil {
					log.Printf("union mount exited with error after kill: %v", err)
				}
			} else {
				log.Printf("union mount did not report exit after SIGKILL")
			}
		}
		logTiming("stop_wait_union_exit", waitUnionExitStarted)
		logTiming("stop_union_total", stopUnionStarted)
	}
	if cfg.onReady != nil {
		readyWaitStarted := time.Now()
		if err := waitForUnionMountReady(ctx, cfg.mountpoint, waitCh, unionMountReadyTimeout, nil); err != nil {
			logTiming("wait_union_mount_ready", readyWaitStarted, "ok=false")
			stopUnion("ready wait failed")
			return err
		}
		logTiming("wait_union_mount_ready", readyWaitStarted, "ok=true")
		safeInvokeReadyCallback(cfg.onReady)
	}

	select {
	case <-ctx.Done():
		stopUnion(ctx.Err().Error())
		return nil
	case err := <-waitCh:
		if err != nil {
			return fmt.Errorf("union mount exited: %w", err)
		}
	}

	return nil
}

func pullImageWithDiscovery(ctx context.Context, imageProviders []serviceInfo, allServices []serviceInfo, cfg config) (serviceInfo, *layerstorepb.PullImageResponse, error) {
	if !cfg.forceLocalFetch && len(imageProviders) > 0 {
		for _, s := range rankServicesForAffinity(imageProviders, cfg.nodeID) {
			attemptStarted := time.Now()
			resp, err := pullImageFromService(ctx, s.endpoint, cfg, false)
			if err == nil {
				logTiming("pull_image_attempt", attemptStarted, "endpoint="+s.endpoint, "source=cached", "force_local_fetch=false", "ok=true")
				log.Printf("using cached image provider endpoint=%s", s.endpoint)
				return s, resp, nil
			}
			logTiming("pull_image_attempt", attemptStarted, "endpoint="+s.endpoint, "source=cached", "force_local_fetch=false", "ok=false")
			log.Printf("cached provider pull failed endpoint=%s: %v", s.endpoint, err)
		}
	}

	for _, s := range rankServicesForAffinity(allServices, cfg.nodeID) {
		attemptStarted := time.Now()
		resp, pullErr := pullImageFromService(ctx, s.endpoint, cfg, cfg.forceLocalFetch)
		if pullErr == nil {
			logTiming("pull_image_attempt", attemptStarted, "endpoint="+s.endpoint, "source=all", "force_local_fetch="+strconv.FormatBool(cfg.forceLocalFetch), "ok=true")
			log.Printf("pulled image on endpoint=%s force_local_fetch=%v", s.endpoint, cfg.forceLocalFetch)
			return s, resp, nil
		}
		logTiming("pull_image_attempt", attemptStarted, "endpoint="+s.endpoint, "source=all", "force_local_fetch="+strconv.FormatBool(cfg.forceLocalFetch), "ok=false")
		log.Printf("pull image failed on endpoint=%s: %v", s.endpoint, pullErr)
	}
	return serviceInfo{}, nil, fmt.Errorf("failed to pull image %s across all discovered services", cfg.image)
}

func pullImageFromService(parentCtx context.Context, endpoint string, cfg config, force bool) (*layerstorepb.PullImageResponse, error) {
	conn, releaseConn, err := sharedGRPCConnCache.Acquire(endpoint, cfg.grpcTimeout, cfg.grpcInsecure)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	client := layerstorepb.NewLayerStoreClient(conn)
	ctx, cancel := withTimeoutFromParent(parentCtx, cfg.pullTimeout)
	defer cancel()
	return client.PullImage(ctx, &layerstorepb.PullImageRequest{
		Image:           cfg.image,
		Tag:             cfg.tag,
		PlatformOs:      cfg.platformOS,
		PlatformArch:    cfg.platformArch,
		PlatformVariant: cfg.platformVariant,
		ForceLocalFetch: force,
	})
}

func discoverImageProviders(services []serviceInfo, chosen serviceInfo, cfg config) ([]serviceInfo, error) {
	out := make([]serviceInfo, 0, len(services))
	seen := make(map[string]struct{}, len(services))
	for _, s := range rankServicesForAffinity(services, cfg.nodeID) {
		if _, ok := seen[s.endpoint]; ok {
			continue
		}
		seen[s.endpoint] = struct{}{}
		out = append(out, s)
	}
	if _, ok := seen[chosen.endpoint]; !ok {
		out = append([]serviceInfo{chosen}, out...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no providers with image available")
	}
	return out, nil
}

func findImageServices(parentCtx context.Context, client discoverypb.ServiceDiscoveryClient, imageKey string, timeout time.Duration) ([]serviceInfo, error) {
	ctx, cancel := withTimeoutFromParent(parentCtx, timeout)
	defer cancel()
	resp, err := client.FindImageProvider(ctx, &discoverypb.FindImageProviderRequest{ImageKey: imageKey})
	if err != nil {
		return nil, err
	}
	services := make([]serviceInfo, 0, len(resp.GetServices()))
	for _, s := range resp.GetServices() {
		endpoint := strings.TrimSpace(s.GetEndpoint())
		if endpoint == "" {
			continue
		}
		services = append(services, serviceInfo{nodeID: s.GetNodeId(), endpoint: endpoint})
	}
	return services, nil
}

func listDiscoveryProviders(parentCtx context.Context, client discoverypb.ServiceDiscoveryClient, timeout time.Duration) ([]serviceInfo, error) {
	ctx, cancel := withTimeoutFromParent(parentCtx, timeout)
	defer cancel()
	resp, err := client.FindProvider(ctx, &discoverypb.FindProviderRequest{})
	if err != nil {
		return nil, err
	}
	services := make([]serviceInfo, 0, len(resp.GetServices()))
	for _, s := range resp.GetServices() {
		endpoint := strings.TrimSpace(s.GetEndpoint())
		if endpoint == "" {
			continue
		}
		services = append(services, serviceInfo{nodeID: s.GetNodeId(), endpoint: endpoint})
	}
	return services, nil
}

func findLayerServicesCached(parentCtx context.Context, discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]serviceInfo, error) {
	cacheKey := layerServiceCacheKey(discoveryAddr, digest)
	if cached, ok := sharedLayerSvcCache.get(cacheKey); ok {
		log.Printf("__AFS_MOUNT_TIMING__ phase=find_layer_services_cached ms=0 digest=%s hit=true services=%d", digest, len(cached))
		return append([]serviceInfo(nil), cached...), nil
	}
	log.Printf("__AFS_MOUNT_TIMING__ phase=find_layer_services_cached ms=0 digest=%s hit=false services=0", digest)

	conn, releaseConn, err := sharedGRPCConnCache.Acquire(discoveryAddr, timeout, insecureTransport)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	client := discoverypb.NewServiceDiscoveryClient(conn)
	ctx, cancel := withTimeoutFromParent(parentCtx, timeout)
	defer cancel()
	findLayerSvcStarted := time.Now()
	resp, err := client.FindProvider(ctx, &discoverypb.FindProviderRequest{LayerDigest: digest})
	if err != nil {
		logTiming("discovery_find_layer_services", findLayerSvcStarted, "digest="+digest, "services=0", "ok=false")
		return nil, err
	}
	out := make([]serviceInfo, 0, len(resp.GetServices()))
	for _, svc := range resp.GetServices() {
		if svc == nil {
			continue
		}
		ep := strings.TrimSpace(svc.GetEndpoint())
		if ep == "" {
			continue
		}
		out = append(out, serviceInfo{
			nodeID:   strings.TrimSpace(svc.GetNodeId()),
			endpoint: ep,
		})
	}
	out = dedupeServiceInfos(out)
	logTiming("discovery_find_layer_services", findLayerSvcStarted, "digest="+digest, "services="+strconv.Itoa(len(out)), "ok=true")
	sharedLayerSvcCache.put(cacheKey, out)
	return append([]serviceInfo(nil), out...), nil
}

func imageResolveCacheKey(cfg config, imageKey string) string {
	return strings.Join([]string{
		cfg.discoveryAddr,
		imageKey,
		strings.TrimSpace(cfg.nodeID),
		fmt.Sprintf("%t", cfg.forceLocalFetch),
	}, "|")
}

func layerServiceCacheKey(discoveryAddr, digest string) string {
	return strings.Join([]string{
		strings.TrimSpace(discoveryAddr),
		strings.TrimSpace(digest),
	}, "|")
}

func rankServicesForAffinity(services []serviceInfo, nodeID string) []serviceInfo {
	local := make([]serviceInfo, 0, len(services))
	remote := make([]serviceInfo, 0, len(services))
	for _, s := range services {
		if nodeID != "" && s.nodeID == nodeID {
			local = append(local, s)
		} else {
			remote = append(remote, s)
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(remote), func(i, j int) { remote[i], remote[j] = remote[j], remote[i] })
	r.Shuffle(len(local), func(i, j int) { local[i], local[j] = local[j], local[i] })
	return append(local, remote...)
}

func dedupeServiceInfos(in []serviceInfo) []serviceInfo {
	seen := make(map[string]struct{}, len(in))
	out := make([]serviceInfo, 0, len(in))
	for _, s := range in {
		ep := strings.TrimSpace(s.endpoint)
		if ep == "" {
			continue
		}
		if _, ok := seen[ep]; ok {
			continue
		}
		seen[ep] = struct{}{}
		out = append(out, serviceInfo{nodeID: strings.TrimSpace(s.nodeID), endpoint: ep})
	}
	return out
}

func (c *imageResolveCache) get(key string) (imageResolveCacheEntry, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.entries[key]
	if !ok {
		return imageResolveCacheEntry{}, false
	}
	if now.After(v.expireAt) {
		delete(c.entries, key)
		return imageResolveCacheEntry{}, false
	}
	return v, true
}

func (c *imageResolveCache) put(key string, entry imageResolveCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= imageResolveCacheMaxEntries {
		c.pruneLocked()
	}
	c.entries[key] = entry
}

func (c *imageResolveCache) pruneLocked() {
	now := time.Now()
	for k, v := range c.entries {
		if now.After(v.expireAt) {
			delete(c.entries, k)
		}
	}
	if len(c.entries) < imageResolveCacheMaxEntries {
		return
	}
	var oldestKey string
	var oldestAt time.Time
	for k, v := range c.entries {
		if oldestKey == "" || v.expireAt.Before(oldestAt) {
			oldestKey = k
			oldestAt = v.expireAt
		}
	}
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

func (c *layerServiceCache) get(key string) ([]serviceInfo, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if now.After(v.expireAt) {
		delete(c.entries, key)
		return nil, false
	}
	return append([]serviceInfo(nil), v.services...), true
}

func (c *layerServiceCache) put(key string, services []serviceInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= layerSvcCacheMaxEntries {
		c.pruneLocked()
	}
	c.entries[key] = layerServiceCacheEntry{
		expireAt: time.Now().Add(layerSvcCacheTTL),
		services: append([]serviceInfo(nil), services...),
	}
}

func (c *layerServiceCache) pruneLocked() {
	now := time.Now()
	for k, v := range c.entries {
		if now.After(v.expireAt) {
			delete(c.entries, k)
		}
	}
	if len(c.entries) < layerSvcCacheMaxEntries {
		return
	}
	var oldestKey string
	var oldestAt time.Time
	for k, v := range c.entries {
		if oldestKey == "" || v.expireAt.Before(oldestAt) {
			oldestKey = k
			oldestAt = v.expireAt
		}
	}
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

func effectiveLayerMountConcurrency(requested int, total int) int {
	if total <= 0 {
		return 1
	}
	if requested <= 0 {
		return 1
	}
	if requested > total {
		return total
	}
	return requested
}

func toLayerReaderServices(in []serviceInfo) []layerreader.ServiceInfo {
	out := make([]layerreader.ServiceInfo, 0, len(in))
	for _, s := range in {
		out = append(out, layerreader.ServiceInfo{
			NodeID:   s.nodeID,
			Endpoint: s.endpoint,
		})
	}
	return out
}

func waitForUnionMountReady(ctx context.Context, mountpoint string, waitCh <-chan error, timeout time.Duration, readyFn func(string) (bool, error)) error {
	if readyFn == nil {
		readyFn = isUnionMountReady
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(unionMountReadyPollInterval)
	defer ticker.Stop()
	var lastReadyErr error
	for {
		if err, exited := tryReadUnionWait(waitCh); exited {
			if err != nil {
				return fmt.Errorf("union mount exited before ready callback: %w", err)
			}
			return fmt.Errorf("union mount exited before ready callback")
		}

		ready, err := readyFn(mountpoint)
		if err == nil && ready {
			return nil
		}
		if err != nil {
			lastReadyErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-deadline.C:
			if lastReadyErr != nil {
				return fmt.Errorf("timeout waiting for union mount ready: %w", lastReadyErr)
			}
			return fmt.Errorf("timeout waiting for union mount ready")
		}
	}
}

func tryReadUnionWait(waitCh <-chan error) (error, bool) {
	select {
	case err := <-waitCh:
		return err, true
	default:
		return nil, false
	}
}

func isUnionMountReady(mountpoint string) (bool, error) {
	if runtime.GOOS == "linux" {
		mounted, err := isMountedLinux(mountpoint)
		if err != nil {
			return false, err
		}
		if !mounted {
			return false, nil
		}
	}
	if _, err := os.Stat(mountpoint); err != nil {
		return false, err
	}
	return true, nil
}

func isMountedLinux(mountpoint string) (bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 5 {
			continue
		}
		if unescapeMountInfoField(fields[4]) == mountpoint {
			return true, nil
		}
	}
	if err := s.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func unescapeMountInfoField(v string) string {
	replacer := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\")
	return replacer.Replace(v)
}

func tryUnmountUnion(goos string, mountpoint string) error {
	var errs []string
	for _, argv := range unmountCandidates(goos, mountpoint) {
		if len(argv) == 0 {
			continue
		}
		cmd := exec.Command(argv[0], argv[1:]...)
		log.Printf("trying unmount command: %s", strings.Join(argv, " "))
		if out, err := cmd.CombinedOutput(); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v (%s)", strings.Join(argv, " "), err, strings.TrimSpace(string(out))))
			continue
		}
		log.Printf("unmount command succeeded: %s", strings.Join(argv, " "))
		return nil
	}
	if len(errs) == 0 {
		return fmt.Errorf("no unmount command candidates for os=%s", goos)
	}
	return errors.New(strings.Join(errs, "; "))
}

func unmountCandidates(goos string, mountpoint string) [][]string {
	switch goos {
	case "linux":
		return [][]string{{"fusermount3", "-u", mountpoint}, {"fusermount", "-u", mountpoint}, {"umount", mountpoint}}
	case "darwin":
		return [][]string{{"umount", mountpoint}, {"diskutil", "unmount", "force", mountpoint}}
	default:
		return [][]string{{"umount", mountpoint}}
	}
}

func mountLayerReader(reader *layerformat.Reader, mountpoint, source, layerDigest string, debug bool, fuseMaxReadAheadBytes int64, cacheStore *pagecache.Store, holdReaper func() func()) (*fuse.Server, *layerfuse.FuseStats, error) {
	var root *layerfuse.DirNode
	var fuseStats *layerfuse.FuseStats
	if cacheStore != nil {
		root, fuseStats = layerfuse.NewRootWithPageCache(reader, cacheStore)
	} else {
		root, fuseStats = layerfuse.NewRoot(reader)
	}
	entryTimeout := 30 * time.Second
	attrTimeout := 30 * time.Second
	negativeTimeout := 5 * time.Second
	// Hold the child reaper while fusefs.Mount spawns fusermount3 subprocess.
	// Without this, the reaper's Wait4(-1) can steal the fusermount3 child
	// before go-fuse's exec.Cmd.Wait() runs, causing "waitid: no child processes".
	var releaseReaper func()
	if holdReaper != nil {
		releaseReaper = holdReaper()
	}
	server, err := fusefs.Mount(mountpoint, root, &fusefs.Options{
		EntryTimeout:    &entryTimeout,
		AttrTimeout:     &attrTimeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			Debug:                debug,
			FsName:               fmt.Sprintf("afslyr:%s", source),
			Name:                 "afslyr",
			Options:              []string{"ro", "exec"},
			MaxWrite:             1 << 20,
			MaxReadAhead:         int(fuseMaxReadAheadBytes),
			EnableSymlinkCaching: true,
		},
	})
	if releaseReaper != nil {
		releaseReaper()
	}
	if err != nil {
		if strings.Contains(err.Error(), "no FUSE mount utility found") {
			return nil, nil, fmt.Errorf("mount fuse: %w (hint: install FUSE runtime)", err)
		}
		return nil, nil, fmt.Errorf("mount fuse: %w", err)
	}
	return server, fuseStats, nil
}

func mountExtraFilesystems(goos string, mountpoint string, enabled bool) ([]string, error) {
	specs := extraMountSpecs(goos, mountpoint, enabled)
	if len(specs) == 0 {
		return nil, nil
	}
	mounted := make([]string, 0, len(specs))
	for _, spec := range specs {
		if err := os.MkdirAll(spec.target, 0o755); err != nil {
			_ = unmountExtraFilesystems(goos, mounted)
			return nil, fmt.Errorf("create extra mountpoint %s: %w", spec.target, err)
		}
		cmd := exec.Command("mount", "--bind", spec.source, spec.target)
		if out, err := cmd.CombinedOutput(); err != nil {
			_ = unmountExtraFilesystems(goos, mounted)
			return nil, fmt.Errorf("bind mount %s -> %s: %v (%s)", spec.source, spec.target, err, strings.TrimSpace(string(out)))
		}
		mounted = append(mounted, spec.target)
		log.Printf("mounted extra filesystem: %s -> %s", spec.source, spec.target)
	}
	return mounted, nil
}

func unmountExtraFilesystems(goos string, targets []string) error {
	if goos != "linux" || len(targets) == 0 {
		return nil
	}
	var errs []string
	for i := len(targets) - 1; i >= 0; i-- {
		target := targets[i]
		cmd := exec.Command("umount", target)
		if out, err := cmd.CombinedOutput(); err != nil {
			errs = append(errs, fmt.Sprintf("umount %s: %v (%s)", target, err, strings.TrimSpace(string(out))))
			continue
		}
		log.Printf("extra filesystem unmounted: %s", target)
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

type extraMountSpec struct {
	source string
	target string
}

func extraMountSpecs(goos string, mountpoint string, enabled bool) []extraMountSpec {
	if !enabled || goos != "linux" {
		return nil
	}
	return []extraMountSpec{
		{source: "/proc", target: filepath.Join(mountpoint, "proc")},
		{source: "/dev", target: filepath.Join(mountpoint, "dev")},
	}
}

func dialGRPC(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("grpc address is required")
	}
	if !insecureTransport {
		return nil, fmt.Errorf("secure transport is not implemented")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func ensureMountpoint(mountpoint string) error {
	st, err := os.Stat(mountpoint)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a directory", mountpoint)
	}
	return nil
}

func resolveWorkDir(workDir string) (dir string, autoCreated bool, err error) {
	if strings.TrimSpace(workDir) == "" {
		tmp, err := os.MkdirTemp("", "afs-image-mount-*")
		if err != nil {
			return "", false, fmt.Errorf("create temp work dir: %w", err)
		}
		return tmp, true, nil
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", false, fmt.Errorf("create work dir: %w", err)
	}
	return workDir, false, nil
}

func buildUnionMountCommand(goos string, layerDirs []string, mountpoint string, writableUpper string, writableWork string, extraDir string) (*exec.Cmd, error) {
	if len(layerDirs) == 0 {
		return nil, fmt.Errorf("no layer directories to compose")
	}
	extraDir = strings.TrimSpace(extraDir)
	ordered := reverseCopy(layerDirs)
	if goos == "linux" {
		if extraDir != "" {
			ordered = append([]string{extraDir}, ordered...)
		}
		lower := strings.Join(ordered, ":")
		opts := []string{"lowerdir=" + lower, "exec"}
		if strings.TrimSpace(writableUpper) != "" {
			if strings.TrimSpace(writableWork) == "" {
				return nil, fmt.Errorf("writable work dir is required for linux upperdir")
			}
			opts = append(opts, "upperdir="+writableUpper, "workdir="+writableWork)
		}
		return exec.Command("fuse-overlayfs", "-f", "-o", strings.Join(opts, ","), mountpoint), nil
	}
	if goos == "darwin" {
		branches := make([]string, 0, len(ordered)+1)
		if strings.TrimSpace(writableUpper) != "" {
			branches = append(branches, writableUpper+"=RW")
		}
		if extraDir != "" {
			branches = append(branches, extraDir+"=RO")
		}
		for _, dir := range ordered {
			branches = append(branches, dir+"=RO")
		}
		binary := "unionfs-fuse"
		if _, err := exec.LookPath(binary); err != nil {
			if _, altErr := exec.LookPath("unionfs"); altErr != nil {
				return nil, fmt.Errorf("unionfs-fuse not found (also tried unionfs)")
			}
			binary = "unionfs"
		}
		return exec.Command(binary, "-f", "-o", "cow,exec", strings.Join(branches, ":"), mountpoint), nil
	}
	return nil, fmt.Errorf("unsupported OS %s; only linux and darwin are supported", goos)
}

func reverseCopy(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func sanitizeForPath(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "layer"
	}
	v = strings.ReplaceAll(v, ":", "_")
	v = strings.ReplaceAll(v, "/", "_")
	return v
}

func shortDigest(digest string) string {
	if len(digest) <= 18 {
		return digest
	}
	return digest[:18]
}

func imageKey(image, tag, platformOS, platformArch, platformVariant string) string {
	return strings.Join([]string{
		strings.TrimSpace(image),
		strings.TrimSpace(tag),
		strings.TrimSpace(platformOS),
		strings.TrimSpace(platformArch),
		strings.TrimSpace(platformVariant),
		"v2",
	}, "|")
}
