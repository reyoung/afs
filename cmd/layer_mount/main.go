package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerfuse"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

func main() {
	cfg := parseFlags()
	log.Printf("starting mount client: image=%s tag=%s mountpoint=%s grpc=%s platform=%s/%s variant=%s force=%v", cfg.image, cfg.tag, cfg.mountpoint, cfg.grpcAddr, cfg.platformOS, cfg.platformArch, cfg.platformVariant, cfg.forcePull)

	if err := ensureMountpoint(cfg.mountpoint); err != nil {
		log.Fatalf("invalid mountpoint: %v", err)
	}

	conn, err := dialGRPC(cfg.grpcAddr, cfg.grpcTimeout, cfg.grpcInsecure)
	if err != nil {
		log.Fatalf("dial gRPC %s: %v", cfg.grpcAddr, err)
	}
	defer conn.Close()

	client := layerstorepb.NewLayerStoreClient(conn)
	if err := runImageMode(client, cfg); err != nil {
		log.Fatalf("mount image: %v", err)
	}
}

type config struct {
	mountpoint      string
	debug           bool
	grpcAddr        string
	grpcTimeout     time.Duration
	grpcMaxChunk    int
	grpcInsecure    bool
	image           string
	tag             string
	platformOS      string
	platformArch    string
	platformVariant string
	forcePull       bool
	pullTimeout     time.Duration
	workDir         string
	keepWorkDir     bool
}

func parseFlags() config {
	cfg := config{}

	flag.StringVar(&cfg.mountpoint, "mountpoint", "", "mount target directory")
	flag.BoolVar(&cfg.debug, "debug", false, "enable go-fuse debug logs")
	flag.StringVar(&cfg.grpcAddr, "grpc-addr", "127.0.0.1:50051", "layerstore gRPC address")
	flag.DurationVar(&cfg.grpcTimeout, "grpc-timeout", 10*time.Second, "timeout for each gRPC read/stat call")
	flag.IntVar(&cfg.grpcMaxChunk, "grpc-max-chunk", 1<<20, "max bytes per gRPC read call")
	flag.BoolVar(&cfg.grpcInsecure, "grpc-insecure", true, "use insecure gRPC transport")
	flag.StringVar(&cfg.image, "image", "", "image reference for remote image mode")
	flag.StringVar(&cfg.tag, "tag", "", "image tag for remote image mode")
	flag.StringVar(&cfg.platformOS, "platform-os", "linux", "target platform os for PullImage")
	flag.StringVar(&cfg.platformArch, "platform-arch", "amd64", "target platform architecture for PullImage")
	flag.StringVar(&cfg.platformVariant, "platform-variant", "", "target platform variant for PullImage")
	flag.BoolVar(&cfg.forcePull, "force-pull", false, "force refresh image metadata from registry")
	flag.DurationVar(&cfg.pullTimeout, "pull-timeout", 20*time.Minute, "timeout for PullImage RPC")
	flag.StringVar(&cfg.workDir, "work-dir", "", "working directory for per-layer mountpoints (default: temp dir)")
	flag.BoolVar(&cfg.keepWorkDir, "keep-work-dir", false, "keep work directory after exit")
	flag.Parse()

	if strings.TrimSpace(cfg.mountpoint) == "" {
		log.Fatal("-mountpoint is required")
	}
	if strings.TrimSpace(cfg.image) == "" {
		log.Fatal("-image is required")
	}
	if cfg.grpcMaxChunk <= 0 {
		log.Fatal("-grpc-max-chunk must be > 0")
	}
	if !cfg.grpcInsecure {
		log.Fatal("only -grpc-insecure=true is supported now")
	}

	return cfg
}

func runImageMode(client layerstorepb.LayerStoreClient, cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.pullTimeout)
	defer cancel()
	log.Printf("requesting PullImage: image=%s tag=%s platform=%s/%s variant=%s force=%v", cfg.image, cfg.tag, cfg.platformOS, cfg.platformArch, cfg.platformVariant, cfg.forcePull)

	resp, err := client.PullImage(ctx, &layerstorepb.PullImageRequest{
		Image:           cfg.image,
		Tag:             cfg.tag,
		PlatformOs:      cfg.platformOS,
		PlatformArch:    cfg.platformArch,
		PlatformVariant: cfg.platformVariant,
		Force:           cfg.forcePull,
	})
	if err != nil {
		return fmt.Errorf("PullImage failed: %w", err)
	}
	if len(resp.GetLayers()) == 0 {
		return fmt.Errorf("image has no layers: %s", cfg.image)
	}
	log.Printf("PullImage done: resolved=%s/%s:%s layers=%d", resp.GetResolvedRegistry(), resp.GetResolvedRepository(), resp.GetResolvedReference(), len(resp.GetLayers()))

	workDir, autoCreated, err := resolveWorkDir(cfg.workDir)
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

	layersRoot := filepath.Join(workDir, "layers")
	if err := os.MkdirAll(layersRoot, 0o755); err != nil {
		return fmt.Errorf("create layer work dir: %w", err)
	}

	type mountedLayer struct {
		digest string
		dir    string
		server *fuse.Server
	}
	mounted := make([]mountedLayer, 0, len(resp.GetLayers()))
	defer func() {
		for i := len(mounted) - 1; i >= 0; i-- {
			_ = mounted[i].server.Unmount()
			mounted[i].server.Wait()
		}
	}()

	layerDirs := make([]string, 0, len(resp.GetLayers()))
	for i, layer := range resp.GetLayers() {
		digest := layer.GetDigest()
		log.Printf("[%d/%d] preparing layer digest=%s cached=%v afsSize=%d", i+1, len(resp.GetLayers()), digest, layer.GetCached(), layer.GetAfsSize())
		mountDir := filepath.Join(layersRoot, fmt.Sprintf("%03d_%s", i+1, sanitizeForPath(shortDigest(digest))))
		if err := os.MkdirAll(mountDir, 0o755); err != nil {
			return fmt.Errorf("create layer mount dir: %w", err)
		}

		remoteReader, err := newRemoteLayerReader(client, digest, cfg.grpcTimeout, cfg.grpcMaxChunk)
		if err != nil {
			return fmt.Errorf("prepare layer %s: %w", digest, err)
		}
		afslReader, err := layerformat.NewReader(remoteReader)
		if err != nil {
			return fmt.Errorf("open layer %s: %w", digest, err)
		}
		source := fmt.Sprintf("grpc:%s/%s", cfg.grpcAddr, digest)
		server, err := mountLayerReader(afslReader, mountDir, source, cfg.debug)
		if err != nil {
			return fmt.Errorf("mount layer %s: %w", digest, err)
		}
		mounted = append(mounted, mountedLayer{digest: digest, dir: mountDir, server: server})
		layerDirs = append(layerDirs, mountDir)
		log.Printf("[%d/%d] mounted layer %s at %s", i+1, len(resp.GetLayers()), digest, mountDir)
	}

	var writableLayerDir string
	if runtime.GOOS == "darwin" {
		writableLayerDir = filepath.Join(workDir, "writable-upper")
		if err := os.MkdirAll(writableLayerDir, 0o755); err != nil {
			return fmt.Errorf("create writable upper dir: %w", err)
		}
		// Always remove writable upper layer on exit for ephemeral write semantics.
		defer func() { _ = os.RemoveAll(writableLayerDir) }()
	}

	unionCmd, err := buildUnionMountCommand(runtime.GOOS, layerDirs, cfg.mountpoint, writableLayerDir)
	if err != nil {
		return err
	}

	log.Printf("starting union mount command: %s %s", unionCmd.Path, strings.Join(unionCmd.Args[1:], " "))
	unionCmd.Stdout = os.Stdout
	unionCmd.Stderr = os.Stderr
	if err := unionCmd.Start(); err != nil {
		return fmt.Errorf("start union mount: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- unionCmd.Wait()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		log.Printf("received %s, stopping union mount", sig)
		if err := tryUnmountUnion(runtime.GOOS, cfg.mountpoint); err != nil {
			log.Printf("explicit union unmount failed: %v", err)
		} else {
			log.Printf("union mount unmounted: %s", cfg.mountpoint)
		}
		_ = unionCmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-waitCh:
			if err != nil {
				log.Printf("union mount exited with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			_ = unionCmd.Process.Kill()
			<-waitCh
		}
	case err := <-waitCh:
		if err != nil {
			return fmt.Errorf("union mount exited: %w", err)
		}
	}

	return nil
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
		return [][]string{
			{"fusermount3", "-u", mountpoint},
			{"fusermount", "-u", mountpoint},
			{"umount", mountpoint},
		}
	case "darwin":
		return [][]string{
			{"umount", mountpoint},
			{"diskutil", "unmount", "force", mountpoint},
		}
	default:
		return [][]string{{"umount", mountpoint}}
	}
}

func mountLayerReader(reader *layerformat.Reader, mountpoint, source string, debug bool) (*fuse.Server, error) {
	root := layerfuse.NewRoot(reader)
	server, err := fusefs.Mount(mountpoint, root, &fusefs.Options{
		MountOptions: fuse.MountOptions{
			Debug:   debug,
			FsName:  fmt.Sprintf("afslyr:%s", source),
			Name:    "afslyr",
			Options: []string{"ro"},
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "no FUSE mount utility found") {
			return nil, fmt.Errorf("mount fuse: %w (hint: install FUSE runtime)", err)
		}
		return nil, fmt.Errorf("mount fuse: %w", err)
	}
	return server, nil
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

func buildUnionMountCommand(goos string, layerDirs []string, mountpoint string, writableUpper string) (*exec.Cmd, error) {
	if len(layerDirs) == 0 {
		return nil, fmt.Errorf("no layer directories to compose")
	}
	ordered := reverseCopy(layerDirs)

	switch goos {
	case "linux":
		lower := strings.Join(ordered, ":")
		return exec.Command("fuse-overlayfs", "-f", "-o", "lowerdir="+lower, mountpoint), nil
	case "darwin":
		branches := make([]string, 0, len(ordered))
		if strings.TrimSpace(writableUpper) != "" {
			branches = append(branches, writableUpper+"=RW")
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
		return exec.Command(binary, "-f", "-o", "cow", strings.Join(branches, ":"), mountpoint), nil
	default:
		return nil, fmt.Errorf("unsupported OS %s; only linux and darwin are supported", goos)
	}
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

type grpcLayerReaderAt struct {
	client   layerstorepb.LayerStoreClient
	digest   string
	size     int64
	timeout  time.Duration
	maxChunk int
}

func newRemoteLayerReader(client layerstorepb.LayerStoreClient, digest string, timeout time.Duration, maxChunk int) (*grpcLayerReaderAt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	log.Printf("stat layer via gRPC: digest=%s", digest)
	stat, err := client.StatLayer(ctx, &layerstorepb.StatLayerRequest{Digest: digest})
	if err != nil {
		return nil, err
	}
	log.Printf("stat layer done: digest=%s afsSize=%d", digest, stat.GetAfsSize())
	return &grpcLayerReaderAt{
		client:   client,
		digest:   digest,
		size:     stat.GetAfsSize(),
		timeout:  timeout,
		maxChunk: maxChunk,
	}, nil
}

func (r *grpcLayerReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}

	maxReadable := int64(len(p))
	if off+maxReadable > r.size {
		maxReadable = r.size - off
	}

	total := 0
	for int64(total) < maxReadable {
		remaining := int(maxReadable - int64(total))
		if remaining > r.maxChunk {
			remaining = r.maxChunk
		}

		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		resp, err := r.client.ReadLayer(ctx, &layerstorepb.ReadLayerRequest{
			Digest: r.digest,
			Offset: off + int64(total),
			Length: int32(remaining),
		})
		cancel()
		if err != nil {
			if total == 0 {
				return 0, err
			}
			return total, err
		}
		n := copy(p[total:], resp.GetData())
		total += n

		if n == 0 || resp.GetEof() {
			break
		}
	}

	if int64(total) < int64(len(p)) {
		return total, io.EOF
	}
	return total, nil
}
