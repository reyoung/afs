package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/reyoung/afs/pkg/debughttp"
	"github.com/reyoung/afs/pkg/pagecache"
	"github.com/reyoung/afs/pkg/pagecachepb"
)

func main() {
	var (
		cacheDir     string
		maxCacheSize string
		udsPath      string
		leaseTimeout time.Duration
		pprofListen  string
	)

	flag.StringVar(&cacheDir, "cache-dir", "/var/cache/afs-page-cache", "cache disk directory")
	flag.StringVar(&maxCacheSize, "max-cache-size", "8GB", "maximum cache capacity (e.g., 8GB, 16GB)")
	flag.StringVar(&udsPath, "uds-path", "/var/run/afs-page-cache.sock", "gRPC Unix domain socket path")
	flag.DurationVar(&leaseTimeout, "lease-timeout", 30*time.Second, "lease timeout duration")
	flag.StringVar(&pprofListen, "pprof-listen", "", "optional HTTP listen address for pprof")
	flag.Parse()

	maxBytes, err := parseSize(maxCacheSize)
	if err != nil {
		log.Fatalf("invalid -max-cache-size %q: %v", maxCacheSize, err)
	}

	log.Printf("starting afs_page_cached: cache-dir=%s max-cache-size=%d uds-path=%s lease-timeout=%s",
		cacheDir, maxBytes, udsPath, leaseTimeout)

	svc, err := pagecache.NewService(cacheDir, maxBytes, leaseTimeout)
	if err != nil {
		log.Fatalf("init page cache service: %v", err)
	}

	// Remove stale socket file
	if err := os.Remove(udsPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("remove stale socket %s: %v", udsPath, err)
	}

	lis, err := net.Listen("unix", udsPath)
	if err != nil {
		log.Fatalf("listen %s: %v", udsPath, err)
	}
	// Allow all users to connect to the socket
	if err := os.Chmod(udsPath, 0o777); err != nil {
		log.Printf("warning: chmod socket %s: %v", udsPath, err)
	}

	grpcServer := grpc.NewServer()
	pagecachepb.RegisterPageCacheServiceServer(grpcServer, svc)
	reflection.Register(grpcServer)

	shutdownPprof := debughttp.StartPprofServer("afs_page_cached", pprofListen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc.StartBackgroundTasks(ctx)

	go func() {
		log.Printf("page cache gRPC listening on unix://%s", udsPath)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	log.Printf("shutting down page cache server")
	grpcServer.GracefulStop()
	if shutdownPprof != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = shutdownPprof(stopCtx)
	}
	_ = os.Remove(udsPath)
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "TB"):
		multiplier = 1 << 40
		s = strings.TrimSuffix(s, "TB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1 << 30
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1 << 20
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1 << 10
		s = strings.TrimSuffix(s, "KB")
	}

	s = strings.TrimSpace(s)
	var val int64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, os.ErrInvalid
		}
		val = val*10 + int64(ch-'0')
	}
	return val * multiplier, nil
}
