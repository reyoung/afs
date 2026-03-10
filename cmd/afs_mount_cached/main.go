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
		ownerLockPath string
		pidFilePath   string
		cacheMaxBytes int64
	)
	flag.StringVar(&cacheDir, "cache-dir", ".cache/afs_mount_cached", "shared spill cache directory")
	flag.StringVar(&sockPath, "sock", "", "unix socket path (default: <cache-dir>/daemon.sock)")
	flag.StringVar(&ownerLockPath, "owner-lock", "", "owner lock path (default: <cache-dir>/daemon.owner.lock)")
	flag.StringVar(&pidFilePath, "pid-file", "", "daemon pid file path (default: <cache-dir>/daemon.pid)")
	flag.Int64Var(&cacheMaxBytes, "cache-max-bytes", 10<<30, "max bytes of shared spill cache")
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
		"starting afs_mount_cached: cache-dir=%s sock=%s owner-lock=%s pid-file=%s cache-max-bytes=%d",
		cacheDir,
		sockPath,
		ownerLockPath,
		pidFilePath,
		cacheMaxBytes,
	)
	if err := spillcache.RunServer(cfg); err != nil {
		log.Fatalf("run cache server: %v", err)
	}
}
