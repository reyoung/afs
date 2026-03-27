package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/reyoung/afs/pkg/afslet"
	"github.com/reyoung/afs/pkg/afsletpb"
	"github.com/reyoung/afs/pkg/bytesize"
	"github.com/reyoung/afs/pkg/debughttp"
	"github.com/reyoung/afs/pkg/filecache"
	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/pagecache"
)

func main() {
	var listenAddr string
	var runcBinary string
	var runcNoPivot bool
	var runcNoNewKeyring bool
	var runcNoCgroupNS bool
	var runcNoPIDNS bool
	var runcNoIPCNS bool
	var runcNoUTSNS bool
	var useSudo bool
	var tarChunk int
	var gracefulTimeout time.Duration
	var defaultDiscoveryAddr string
	var tempDir string
	var limitCPUCores int64
	var limitMemoryMB int64
	var layerMountConcurrency int
	var mountPprofListen string
	var fuseMaxReadAhead string
	var sharedCatalogMaxImages int
	var sharedCatalogIdleTTL time.Duration
	var pprofListen string
	var pageCacheDir string
	var pageCacheMaxSize string
	var elfCacheDir string
	var elfCacheMaxSize string
	var elfCacheMaxFileSize string
	var mountMode string

	flag.StringVar(&listenAddr, "listen", ":61051", "gRPC listen address")
	flag.StringVar(&runcBinary, "runc-binary", "afs_runc", "afs_runc binary path")
	flag.BoolVar(&runcNoPivot, "runc-no-pivot", false, "pass --no-pivot to afs_runc")
	flag.BoolVar(&runcNoNewKeyring, "runc-no-new-keyring", false, "pass --no-new-keyring to afs_runc")
	flag.BoolVar(&runcNoCgroupNS, "runc-no-cgroup-ns", false, "do not create cgroup namespace in afs_runc spec")
	flag.BoolVar(&runcNoPIDNS, "runc-no-pid-ns", false, "do not create pid namespace in afs_runc spec")
	flag.BoolVar(&runcNoIPCNS, "runc-no-ipc-ns", false, "do not create ipc namespace in afs_runc spec")
	flag.BoolVar(&runcNoUTSNS, "runc-no-uts-ns", false, "do not create uts namespace in afs_runc spec")
	flag.BoolVar(&useSudo, "sudo-binaries", false, "run afs_runc through sudo")
	flag.IntVar(&tarChunk, "tar-chunk", 256*1024, "tar.gz stream chunk size in bytes")
	flag.DurationVar(&gracefulTimeout, "graceful-timeout", 10*time.Second, "max wait for graceful gRPC shutdown before force stop")
	flag.StringVar(&defaultDiscoveryAddr, "discovery-addr", "", "default discovery address used when request does not specify one")
	flag.StringVar(&tempDir, "temp-dir", "", "base temp directory for afslet sessions (default: system temp dir)")
	flag.Int64Var(&limitCPUCores, "limit-cpu", 1, "total allocatable CPU cores for afslet admission control")
	flag.Int64Var(&limitMemoryMB, "limit-memory-mb", 256, "total allocatable memory (MB) for afslet admission control")
	flag.IntVar(&layerMountConcurrency, "layer-mount-concurrency", 1, "max number of layers to prepare/mount concurrently")
	flag.StringVar(&mountPprofListen, "mount-pprof-listen", "", "optional HTTP listen address for mount pprof")
	flag.StringVar(&fuseMaxReadAhead, "fuse-max-read-ahead", "8M", "max FUSE read-ahead bytes, e.g. 8M, 16M, 32MiB")
	flag.IntVar(&sharedCatalogMaxImages, "shared-catalog-max-images", 64, "max idle+active image directories kept in shared FUSE catalog")
	flag.DurationVar(&sharedCatalogIdleTTL, "shared-catalog-idle-ttl", 10*time.Minute, "idle TTL before evicting an unused image directory from shared FUSE catalog")
	flag.StringVar(&pprofListen, "pprof-listen", "", "optional HTTP listen address for pprof, e.g. 127.0.0.1:6062")
	flag.StringVar(&pageCacheDir, "page-cache-dir", "", "directory for on-disk page cache (empty=disabled)")
	flag.StringVar(&pageCacheMaxSize, "page-cache-max-size", "8GB", "max size for on-disk page cache, e.g. 8GB, 16GiB")
	flag.StringVar(&elfCacheDir, "elf-cache-dir", "", "directory for on-disk ELF file cache (empty=disabled)")
	flag.StringVar(&elfCacheMaxSize, "elf-cache-max-size", "1GB", "max total size for ELF file cache")
	flag.StringVar(&elfCacheMaxFileSize, "elf-cache-max-file-size", "32MB", "max single file size for ELF file cache")
	flag.StringVar(&mountMode, "mount-mode", "unified-koverlay", "mount mode: unified-koverlay")
	flag.Parse()

	fuseMaxReadAheadBytes, err := bytesize.Parse(fuseMaxReadAhead)
	if err != nil {
		log.Fatalf("invalid -fuse-max-read-ahead: %v", err)
	}

	var pageCacheStore *pagecache.Store
	var elfCacheStore *filecache.Store
	if pageCacheDir != "" {
		maxBytes, parseErr := bytesize.Parse(pageCacheMaxSize)
		if parseErr != nil {
			log.Fatalf("invalid -page-cache-max-size: %v", parseErr)
		}
		var storeErr error
		pageCacheStore, storeErr = pagecache.NewStore(pageCacheDir, maxBytes)
		if storeErr != nil {
			log.Fatalf("init page cache store: %v", storeErr)
		}
		defer pageCacheStore.Close()
		log.Printf("page cache store initialized: dir=%s max-size=%s", pageCacheDir, pageCacheMaxSize)
	}
	if elfCacheDir != "" {
		maxBytes, parseErr := bytesize.Parse(elfCacheMaxSize)
		if parseErr != nil {
			log.Fatalf("invalid -elf-cache-max-size: %v", parseErr)
		}
		maxFileBytes, parseErr := bytesize.Parse(elfCacheMaxFileSize)
		if parseErr != nil {
			log.Fatalf("invalid -elf-cache-max-file-size: %v", parseErr)
		}
		var storeErr error
		elfCacheStore, storeErr = filecache.NewStore(elfCacheDir, maxBytes, maxFileBytes)
		if storeErr != nil {
			log.Fatalf("init ELF cache store: %v", storeErr)
		}
		defer elfCacheStore.Close()
		log.Printf("ELF cache store initialized: dir=%s max-size=%s max-file-size=%s", elfCacheDir, elfCacheMaxSize, elfCacheMaxFileSize)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	svc := afslet.NewService(afslet.Config{
		RuncBinary:             runcBinary,
		RuncNoPivot:            runcNoPivot,
		RuncNoNewKeyring:       runcNoNewKeyring,
		RuncNoCgroupNS:         runcNoCgroupNS,
		RuncNoPIDNS:            runcNoPIDNS,
		RuncNoIPCNS:            runcNoIPCNS,
		RuncNoUTSNS:            runcNoUTSNS,
		UseSudo:                useSudo,
		TarChunk:               tarChunk,
		DefaultDiscovery:       defaultDiscoveryAddr,
		TempDir:                tempDir,
		SharedMountRoot:        sharedMountRoot(tempDir),
		LimitCPUCores:          limitCPUCores,
		LimitMemoryMB:          limitMemoryMB,
		LayerMountConcurrency:  layerMountConcurrency,
		MountPprofListen:       mountPprofListen,
		FUSEMaxReadAheadBytes:  fuseMaxReadAheadBytes,
		SharedCatalogMaxImages: sharedCatalogMaxImages,
		SharedCatalogIdleTTL:   sharedCatalogIdleTTL,
		PageCacheStore:         pageCacheStore,
		ELFCacheStore:          elfCacheStore,
		TOCCache:               layerformat.NewTOCCache(256),
		MountMode:              mountMode,
	})

	grpcServer := grpc.NewServer()
	afsletpb.RegisterAfsletServer(grpcServer, svc)
	reflection.Register(grpcServer)
	shutdownPprof := debughttp.StartPprofServer("afslet", pprofListen)
	stopReaper := afslet.StartChildReaper(log.Printf)
	defer stopReaper()

	go func() {
		log.Printf("afslet listening on %s", listenAddr)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Fatalf("grpc serve: %v", serveErr)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down afslet (graceful-timeout=%s)", gracefulTimeout)
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("afslet stopped gracefully")
	case <-time.After(gracefulTimeout):
		log.Printf("graceful stop timed out, forcing stop")
		grpcServer.Stop()
	}
	if shutdownPprof != nil {
		ctx, cancel := context.WithTimeout(context.Background(), gracefulTimeout)
		defer cancel()
		_ = shutdownPprof(ctx)
	}
}

func sharedMountRoot(tempDir string) string {
	if strings.TrimSpace(tempDir) == "" {
		return ""
	}
	return filepath.Join(tempDir, "shared-catalog")
}
