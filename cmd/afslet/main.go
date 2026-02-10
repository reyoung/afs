package main

import (
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
)

func main() {
	var listenAddr string
	var mountBinary string
	var runcBinary string
	var useSudo bool
	var tarChunk int
	var gracefulTimeout time.Duration
	var defaultDiscoveryAddr string

	flag.StringVar(&listenAddr, "listen", ":61051", "gRPC listen address")
	flag.StringVar(&mountBinary, "mount-binary", "afs_mount", "afs_mount binary path")
	flag.StringVar(&runcBinary, "runc-binary", "afs_runc", "afs_runc binary path")
	flag.BoolVar(&useSudo, "sudo-binaries", false, "run afs_mount/afs_runc through sudo")
	flag.IntVar(&tarChunk, "tar-chunk", 256*1024, "tar.gz stream chunk size in bytes")
	flag.DurationVar(&gracefulTimeout, "graceful-timeout", 10*time.Second, "max wait for graceful gRPC shutdown before force stop")
	flag.StringVar(&defaultDiscoveryAddr, "discovery-addr", "", "default discovery address used when request does not specify one")
	flag.Parse()

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	svc := afslet.NewService(afslet.Config{
		MountBinary:      mountBinary,
		RuncBinary:       runcBinary,
		UseSudo:          useSudo,
		TarChunk:         tarChunk,
		DefaultDiscovery: defaultDiscoveryAddr,
	})

	grpcServer := grpc.NewServer()
	afsletpb.RegisterAfsletServer(grpcServer, svc)
	reflection.Register(grpcServer)

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
}
