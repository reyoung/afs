package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/reyoung/afs/pkg/afslet"
	"github.com/reyoung/afs/pkg/afsletpb"
	"github.com/reyoung/afs/pkg/bytesize"
	"github.com/reyoung/afs/pkg/debughttp"
)

func main() {
	var listenAddr string
	var mountBinary string
	var mountInProcess bool
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
	var pprofListen string

	flag.StringVar(&listenAddr, "listen", ":61051", "gRPC listen address")
	flag.StringVar(&mountBinary, "mount-binary", "afs_mount", "afs_mount binary path")
	flag.BoolVar(&mountInProcess, "mount-in-process", false, "run mount flow in-process instead of spawning afs_mount binary")
	flag.StringVar(&runcBinary, "runc-binary", "afs_runc", "afs_runc binary path")
	flag.BoolVar(&runcNoPivot, "runc-no-pivot", false, "pass --no-pivot to afs_runc")
	flag.BoolVar(&runcNoNewKeyring, "runc-no-new-keyring", false, "pass --no-new-keyring to afs_runc")
	flag.BoolVar(&runcNoCgroupNS, "runc-no-cgroup-ns", false, "do not create cgroup namespace in afs_runc spec")
	flag.BoolVar(&runcNoPIDNS, "runc-no-pid-ns", false, "do not create pid namespace in afs_runc spec")
	flag.BoolVar(&runcNoIPCNS, "runc-no-ipc-ns", false, "do not create ipc namespace in afs_runc spec")
	flag.BoolVar(&runcNoUTSNS, "runc-no-uts-ns", false, "do not create uts namespace in afs_runc spec")
	flag.BoolVar(&useSudo, "sudo-binaries", false, "run afs_mount/afs_runc through sudo")
	flag.IntVar(&tarChunk, "tar-chunk", 256*1024, "tar.gz stream chunk size in bytes")
	flag.DurationVar(&gracefulTimeout, "graceful-timeout", 10*time.Second, "max wait for graceful gRPC shutdown before force stop")
	flag.StringVar(&defaultDiscoveryAddr, "discovery-addr", "", "default discovery address used when request does not specify one")
	flag.StringVar(&tempDir, "temp-dir", "", "base temp directory for afslet sessions (default: system temp dir)")
	flag.Int64Var(&limitCPUCores, "limit-cpu", 1, "total allocatable CPU cores for afslet admission control")
	flag.Int64Var(&limitMemoryMB, "limit-memory-mb", 256, "total allocatable memory (MB) for afslet admission control")
	flag.IntVar(&layerMountConcurrency, "layer-mount-concurrency", 1, "max number of layers to prepare/mount concurrently in afs_mount")
	flag.StringVar(&mountPprofListen, "mount-pprof-listen", "", "optional HTTP listen address for afs_mount pprof")
	flag.StringVar(&fuseMaxReadAhead, "fuse-max-read-ahead", "8M", "max FUSE read-ahead bytes for afs_mount, e.g. 8M, 16M, 32MiB")
	flag.StringVar(&pprofListen, "pprof-listen", "", "optional HTTP listen address for pprof, e.g. 127.0.0.1:6062")
	flag.Parse()

	fuseMaxReadAheadBytes, err := bytesize.Parse(fuseMaxReadAhead)
	if err != nil {
		log.Fatalf("invalid -fuse-max-read-ahead: %v", err)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	svc := afslet.NewService(afslet.Config{
		MountBinary:                 mountBinary,
		MountInProcess:              mountInProcess,
		RuncBinary:                  runcBinary,
		RuncNoPivot:                 runcNoPivot,
		RuncNoNewKeyring:            runcNoNewKeyring,
		RuncNoCgroupNS:              runcNoCgroupNS,
		RuncNoPIDNS:                 runcNoPIDNS,
		RuncNoIPCNS:                 runcNoIPCNS,
		RuncNoUTSNS:                 runcNoUTSNS,
		UseSudo:                     useSudo,
		TarChunk:                    tarChunk,
		DefaultDiscovery:            defaultDiscoveryAddr,
		TempDir:                     tempDir,
		LimitCPUCores:               limitCPUCores,
		LimitMemoryMB:               limitMemoryMB,
		LayerMountConcurrency:       layerMountConcurrency,
		MountPprofListen:            mountPprofListen,
		FUSEMaxReadAheadBytes:       fuseMaxReadAheadBytes,
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
