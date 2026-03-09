package main

import (
	"flag"
	"log"
	"path/filepath"

	"github.com/reyoung/afs/pkg/spillcache"
)

func main() {
	var (
		cacheDir      string
		sockPath      string
		cacheMaxBytes int64
	)
	flag.StringVar(&cacheDir, "cache-dir", ".cache/afs_mount_cached", "shared spill cache directory")
	flag.StringVar(&sockPath, "sock", "", "unix socket path (default: <cache-dir>/daemon.sock)")
	flag.Int64Var(&cacheMaxBytes, "cache-max-bytes", 10<<30, "max bytes of shared spill cache")
	flag.Parse()

	if sockPath == "" {
		sockPath = filepath.Join(cacheDir, "daemon.sock")
	}
	cfg := spillcache.ServerConfig{CacheDir: cacheDir, SockPath: sockPath, MaxBytes: cacheMaxBytes}
	log.Printf("starting afs_mount_cached: cache-dir=%s sock=%s cache-max-bytes=%d", cacheDir, sockPath, cacheMaxBytes)
	if err := spillcache.RunServer(cfg); err != nil {
		log.Fatalf("run cache server: %v", err)
	}
}
