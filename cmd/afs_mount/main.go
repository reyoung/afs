package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/reyoung/afs/pkg/afsmount"
)

func main() {
	cfg := parseFlags()
	log.Printf("starting mount client: image=%s tag=%s mountpoint=%s discovery=%s node-id=%s platform=%s/%s variant=%s force_local_fetch=%v", cfg.Image, cfg.Tag, cfg.Mountpoint, cfg.DiscoveryAddr, cfg.NodeID, cfg.PlatformOS, cfg.PlatformArch, cfg.PlatformVariant, cfg.ForceLocalFetch)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := afsmount.Run(ctx, cfg); err != nil {
		log.Fatalf("mount image: %v", err)
	}
}

func parseFlags() afsmount.Config {
	cfg := afsmount.Config{}

	flag.StringVar(&cfg.Mountpoint, "mountpoint", "", "mount target directory")
	flag.BoolVar(&cfg.Debug, "debug", false, "enable go-fuse debug logs")
	flag.BoolVar(&cfg.MountProcDev, "mount-proc-dev", true, "mount /proc and /dev into mounted rootfs (linux only)")
	flag.BoolVar(&cfg.NoSpillCache, "no-spill-cache", false, "disable on-disk spill cache reuse for decompressed file payloads")
	flag.StringVar(&cfg.ExtraDir, "extra-dir", "", "extra read-only directory inserted between writable upper and image layers")
	flag.StringVar(&cfg.DiscoveryAddr, "discovery-addr", "127.0.0.1:60051", "service discovery gRPC address")
	flag.DurationVar(&cfg.GRPCTimeout, "grpc-timeout", afsmount.DefaultGRPCTimeout, "timeout for each gRPC call")
	flag.IntVar(&cfg.GRPCMaxChunk, "grpc-max-chunk", 4<<20, "max bytes per gRPC read call")
	flag.BoolVar(&cfg.GRPCInsecure, "grpc-insecure", true, "use insecure gRPC transport")
	flag.StringVar(&cfg.NodeID, "node-id", "", "node affinity id for preferring local layerstore")
	flag.StringVar(&cfg.Image, "image", "", "image reference for remote image mode")
	flag.StringVar(&cfg.Tag, "tag", "", "image tag for remote image mode")
	flag.StringVar(&cfg.PlatformOS, "platform-os", "linux", "target platform os for image resolution")
	flag.StringVar(&cfg.PlatformArch, "platform-arch", "amd64", "target platform architecture for image resolution")
	flag.StringVar(&cfg.PlatformVariant, "platform-variant", "", "target platform variant for image resolution")
	flag.BoolVar(&cfg.ForceLocalFetch, "force-local-fetch", false, "force local layer fetch on this node")
	flag.DurationVar(&cfg.PullTimeout, "pull-timeout", 20*time.Minute, "timeout for layer ensure RPC")
	flag.StringVar(&cfg.WorkDir, "work-dir", "", "working directory for per-layer mountpoints (default: temp dir)")
	flag.BoolVar(&cfg.KeepWorkDir, "keep-work-dir", false, "keep work directory after exit")
	flag.StringVar(&cfg.FUSETempDir, "fuse-temp-dir", "", "local temp directory for decompressed file spill during FUSE reads")
	flag.BoolVar(&cfg.SharedSpillCacheEnabled, "shared-spill-cache", false, "enable shared multi-process spill cache daemon")
	flag.StringVar(&cfg.SharedSpillCacheDir, "shared-spill-cache-dir", ".cache/afs_mount_cached", "shared spill cache root directory")
	flag.StringVar(&cfg.SharedSpillCacheSock, "shared-spill-cache-sock", "", "shared spill cache unix socket path (default: <shared-spill-cache-dir>/daemon.sock)")
	flag.Int64Var(&cfg.SharedSpillCacheMaxBytes, "shared-spill-cache-max-bytes", 10<<30, "shared spill cache max bytes")
	flag.StringVar(&cfg.SharedSpillCacheBinaryPath, "shared-spill-cache-binary", "afs_mount_cached", "path of shared spill cache daemon binary")
	flag.IntVar(&cfg.LayerMountConcurrency, "layer-mount-concurrency", 1, "max number of layers to prepare/mount concurrently")
	flag.Parse()

	if strings.TrimSpace(cfg.Mountpoint) == "" {
		log.Fatal("-mountpoint is required")
	}
	if strings.TrimSpace(cfg.Image) == "" {
		log.Fatal("-image is required")
	}
	if cfg.LayerMountConcurrency <= 0 {
		log.Fatal("-layer-mount-concurrency must be > 0")
	}
	if cfg.GRPCMaxChunk <= 0 {
		log.Fatal("-grpc-max-chunk must be > 0")
	}
	if !cfg.GRPCInsecure {
		log.Fatal("only -grpc-insecure=true is supported now")
	}
	return cfg
}

func imageKey(image, tag, platformOS, platformArch, platformVariant string) string {
	return strings.Join([]string{
		strings.TrimSpace(image),
		strings.TrimSpace(tag),
		strings.TrimSpace(platformOS),
		strings.TrimSpace(platformArch),
		strings.TrimSpace(platformVariant),
	}, "|")
}
