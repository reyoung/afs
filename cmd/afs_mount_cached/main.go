package main

import (
	"context"
	"flag"
	"log"
	"path/filepath"
	"time"

	"github.com/reyoung/afs/pkg/debughttp"
	"github.com/reyoung/afs/pkg/spillcache"
)

func main() {
	var (
		cacheDir      string
		sockPath      string
		ownerLockPath string
		pidFilePath   string
		cacheMaxBytes int64
		pprofListen   string
	)
	flag.StringVar(&cacheDir, "cache-dir", ".cache/afs_mount_cached", "shared spill cache directory")
	flag.StringVar(&sockPath, "sock", "", "unix socket path (default: <cache-dir>/daemon.sock)")
	flag.StringVar(&ownerLockPath, "owner-lock", "", "owner lock path (default: <cache-dir>/daemon.owner.lock)")
	flag.StringVar(&pidFilePath, "pid-file", "", "daemon pid file path (default: <cache-dir>/daemon.pid)")
	flag.Int64Var(&cacheMaxBytes, "cache-max-bytes", 10<<30, "max bytes of shared spill cache")
	flag.StringVar(&pprofListen, "pprof-listen", "", "optional HTTP listen address for pprof, e.g. 127.0.0.1:6061")
	flag.Parse()

	if sockPath == "" {
		sockPath = filepath.Join(cacheDir, "daemon.sock")
	}
	if ownerLockPath == "" {
		ownerLockPath = filepath.Join(cacheDir, "daemon.owner.lock")
	}
	if pidFilePath == "" {
		pidFilePath = filepath.Join(cacheDir, "daemon.pid")
	}
	cfg := spillcache.ServerConfig{
		CacheDir:      cacheDir,
		SockPath:      sockPath,
		MaxBytes:      cacheMaxBytes,
		OwnerLockPath: ownerLockPath,
		PIDFilePath:   pidFilePath,
	}
	log.Printf(
		"starting afs_mount_cached: cache-dir=%s sock=%s owner-lock=%s pid-file=%s cache-max-bytes=%d pprof-listen=%s",
		cacheDir,
		sockPath,
		ownerLockPath,
		pidFilePath,
		cacheMaxBytes,
		pprofListen,
	)
	shutdownPprof := debughttp.StartPprofServer("afs_mount_cached", pprofListen)
	if shutdownPprof != nil {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownPprof(ctx)
		}()
	}
	if err := spillcache.RunServer(cfg); err != nil {
		log.Fatalf("run cache server: %v", err)
	}
}
