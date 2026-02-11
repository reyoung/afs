package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerfuse"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

type config struct {
	mountpoint      string
	debug           bool
	mountProcDev    bool
	extraDir        string
	discoveryAddr   string
	grpcTimeout     time.Duration
	grpcMaxChunk    int
	grpcInsecure    bool
	nodeID          string
	image           string
	tag             string
	platformOS      string
	platformArch    string
	platformVariant string
	forceLocalFetch bool
	pullTimeout     time.Duration
	workDir         string
	keepWorkDir     bool
	fuseTempDir     string
}

type serviceInfo struct {
	nodeID   string
	endpoint string
}

func main() {
	cfg := parseFlags()
	log.Printf("starting mount client: image=%s tag=%s mountpoint=%s discovery=%s node-id=%s platform=%s/%s variant=%s force_local_fetch=%v", cfg.image, cfg.tag, cfg.mountpoint, cfg.discoveryAddr, cfg.nodeID, cfg.platformOS, cfg.platformArch, cfg.platformVariant, cfg.forceLocalFetch)

	if err := ensureMountpoint(cfg.mountpoint); err != nil {
		log.Fatalf("invalid mountpoint: %v", err)
	}

	discoveryConn, err := dialGRPC(cfg.discoveryAddr, cfg.grpcTimeout, cfg.grpcInsecure)
	if err != nil {
		log.Fatalf("dial discovery gRPC %s: %v", cfg.discoveryAddr, err)
	}
	defer discoveryConn.Close()

	discoveryClient := discoverypb.NewServiceDiscoveryClient(discoveryConn)
	if err := runImageMode(discoveryClient, cfg); err != nil {
		log.Fatalf("mount image: %v", err)
	}
}

func parseFlags() config {
	cfg := config{}

	flag.StringVar(&cfg.mountpoint, "mountpoint", "", "mount target directory")
	flag.BoolVar(&cfg.debug, "debug", false, "enable go-fuse debug logs")
	flag.BoolVar(&cfg.mountProcDev, "mount-proc-dev", true, "mount /proc and /dev into mounted rootfs (linux only)")
	flag.StringVar(&cfg.extraDir, "extra-dir", "", "extra read-only directory inserted between writable upper and image layers")
	flag.StringVar(&cfg.discoveryAddr, "discovery-addr", "127.0.0.1:60051", "service discovery gRPC address")
	flag.DurationVar(&cfg.grpcTimeout, "grpc-timeout", 10*time.Second, "timeout for each gRPC call")
	flag.IntVar(&cfg.grpcMaxChunk, "grpc-max-chunk", 1<<20, "max bytes per gRPC read call")
	flag.BoolVar(&cfg.grpcInsecure, "grpc-insecure", true, "use insecure gRPC transport")
	flag.StringVar(&cfg.nodeID, "node-id", "", "node affinity id for preferring local layerstore")
	flag.StringVar(&cfg.image, "image", "", "image reference for remote image mode")
	flag.StringVar(&cfg.tag, "tag", "", "image tag for remote image mode")
	flag.StringVar(&cfg.platformOS, "platform-os", "linux", "target platform os for PullImage")
	flag.StringVar(&cfg.platformArch, "platform-arch", "amd64", "target platform architecture for PullImage")
	flag.StringVar(&cfg.platformVariant, "platform-variant", "", "target platform variant for PullImage")
	flag.BoolVar(&cfg.forceLocalFetch, "force-local-fetch", false, "force local layer fetch on this node")
	flag.DurationVar(&cfg.pullTimeout, "pull-timeout", 20*time.Minute, "timeout for PullImage RPC")
	flag.StringVar(&cfg.workDir, "work-dir", "", "working directory for per-layer mountpoints (default: temp dir)")
	flag.BoolVar(&cfg.keepWorkDir, "keep-work-dir", false, "keep work directory after exit")
	flag.StringVar(&cfg.fuseTempDir, "fuse-temp-dir", "", "local temp directory for decompressed file spill during FUSE reads")
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
	if strings.TrimSpace(cfg.extraDir) != "" {
		if err := ensureMountpoint(cfg.extraDir); err != nil {
			log.Fatalf("invalid -extra-dir: %v", err)
		}
	}
	return cfg
}

func runImageMode(discoveryClient discoverypb.ServiceDiscoveryClient, cfg config) error {
	imageProviders, err := findImageServices(discoveryClient, imageKey(cfg.image, cfg.tag, cfg.platformOS, cfg.platformArch, cfg.platformVariant), cfg.grpcTimeout)
	if err != nil {
		return err
	}
	log.Printf("discovery returned %d providers with image", len(imageProviders))

	allServices := imageProviders
	if len(allServices) == 0 {
		allServices, err = findImageServices(discoveryClient, "", cfg.grpcTimeout)
		if err != nil {
			return err
		}
		if len(allServices) == 0 {
			return fmt.Errorf("no layerstore services registered in discovery")
		}
		log.Printf("no cached image providers found; fallback to %d total services", len(allServices))
	}

	chosen, pullResp, err := pullImageWithDiscovery(imageProviders, allServices, cfg)
	if err != nil {
		return err
	}
	log.Printf("image resolved from endpoint=%s resolved=%s/%s:%s layers=%d", chosen.endpoint, pullResp.GetResolvedRegistry(), pullResp.GetResolvedRepository(), pullResp.GetResolvedReference(), len(pullResp.GetLayers()))
	providers, err := discoverImageProviders(allServices, chosen, cfg)
	if err != nil {
		return err
	}
	log.Printf("image providers available for failover: %d", len(providers))

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
	mounted := make([]mountedLayer, 0, len(pullResp.GetLayers()))
	defer func() {
		for i := len(mounted) - 1; i >= 0; i-- {
			_ = mounted[i].server.Unmount()
			mounted[i].server.Wait()
		}
	}()

	layerDirs := make([]string, 0, len(pullResp.GetLayers()))
	for i, layer := range pullResp.GetLayers() {
		digest := layer.GetDigest()
		log.Printf("[%d/%d] preparing layer digest=%s", i+1, len(pullResp.GetLayers()), digest)
		mountDir := filepath.Join(layersRoot, fmt.Sprintf("%03d_%s", i+1, sanitizeForPath(shortDigest(digest))))
		if err := os.MkdirAll(mountDir, 0o755); err != nil {
			return fmt.Errorf("create layer mount dir: %w", err)
		}

		reader, err := newDiscoveryBackedLayerReader(cfg, digest, chosen.endpoint, providers)
		if err != nil {
			return fmt.Errorf("prepare discovery-backed layer reader for %s: %w", digest, err)
		}
		afslReader, err := layerformat.NewReader(reader)
		if err != nil {
			return fmt.Errorf("open layer %s: %w", digest, err)
		}
		server, err := mountLayerReader(afslReader, mountDir, "discovery:"+digest, cfg.debug, cfg.fuseTempDir)
		if err != nil {
			return fmt.Errorf("mount layer %s: %w", digest, err)
		}
		mounted = append(mounted, mountedLayer{digest: digest, dir: mountDir, server: server})
		layerDirs = append(layerDirs, mountDir)
		log.Printf("[%d/%d] mounted layer %s at %s", i+1, len(pullResp.GetLayers()), digest, mountDir)
	}

	var writableLayerDir string
	var writableWorkDir string
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		writableLayerDir = filepath.Join(workDir, "writable-upper")
		if err := os.MkdirAll(writableLayerDir, 0o755); err != nil {
			return fmt.Errorf("create writable upper dir: %w", err)
		}
	}
	if runtime.GOOS == "linux" {
		writableWorkDir = filepath.Join(workDir, "writable-work")
		if err := os.MkdirAll(writableWorkDir, 0o755); err != nil {
			return fmt.Errorf("create writable work dir: %w", err)
		}
	}

	unionCmd, err := buildUnionMountCommand(runtime.GOOS, layerDirs, cfg.mountpoint, writableLayerDir, writableWorkDir, cfg.extraDir)
	if err != nil {
		return err
	}
	log.Printf("starting union mount command: %s %s", unionCmd.Path, strings.Join(unionCmd.Args[1:], " "))
	unionCmd.Stdout = os.Stdout
	unionCmd.Stderr = os.Stderr
	if err := unionCmd.Start(); err != nil {
		return fmt.Errorf("start union mount: %w", err)
	}
	extraMountTargets, err := mountExtraFilesystems(runtime.GOOS, cfg.mountpoint, cfg.mountProcDev)
	if err != nil {
		_ = unionCmd.Process.Signal(syscall.SIGTERM)
		_ = tryUnmountUnion(runtime.GOOS, cfg.mountpoint)
		return err
	}
	defer func() {
		if err := unmountExtraFilesystems(runtime.GOOS, extraMountTargets); err != nil {
			log.Printf("unmount extra filesystems failed: %v", err)
		}
	}()

	waitCh := make(chan error, 1)
	go func() { waitCh <- unionCmd.Wait() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		log.Printf("received %s, stopping union mount", sig)
		if err := unmountExtraFilesystems(runtime.GOOS, extraMountTargets); err != nil {
			log.Printf("explicit extra unmount failed: %v", err)
		}
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

func pullImageWithDiscovery(imageProviders []serviceInfo, allServices []serviceInfo, cfg config) (serviceInfo, *layerstorepb.PullImageResponse, error) {
	if !cfg.forceLocalFetch && len(imageProviders) > 0 {
		for _, s := range rankServicesForAffinity(imageProviders, cfg.nodeID) {
			resp, err := pullImageFromService(s.endpoint, cfg, false)
			if err == nil {
				log.Printf("using cached image provider endpoint=%s", s.endpoint)
				return s, resp, nil
			}
			log.Printf("cached provider pull failed endpoint=%s: %v", s.endpoint, err)
		}
	}

	for _, s := range rankServicesForAffinity(allServices, cfg.nodeID) {
		resp, pullErr := pullImageFromService(s.endpoint, cfg, cfg.forceLocalFetch)
		if pullErr == nil {
			log.Printf("pulled image on endpoint=%s force_local_fetch=%v", s.endpoint, cfg.forceLocalFetch)
			return s, resp, nil
		}
		log.Printf("pull image failed on endpoint=%s: %v", s.endpoint, pullErr)
	}
	return serviceInfo{}, nil, fmt.Errorf("failed to pull image %s across all discovered services", cfg.image)
}

func pullImageFromService(endpoint string, cfg config, force bool) (*layerstorepb.PullImageResponse, error) {
	conn, err := dialGRPC(endpoint, cfg.grpcTimeout, cfg.grpcInsecure)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := layerstorepb.NewLayerStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.pullTimeout)
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

func findImageServices(client discoverypb.ServiceDiscoveryClient, imageKey string, timeout time.Duration) ([]serviceInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := client.FindImage(ctx, &discoverypb.FindImageRequest{ImageKey: imageKey})
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

func newDiscoveryBackedLayerReader(cfg config, digest string, bootstrapEndpoint string, providers []serviceInfo) (*discoveryLayerReaderAt, error) {
	r := &discoveryLayerReaderAt{
		cfg:       cfg,
		digest:    digest,
		bootstrap: bootstrapEndpoint,
		providers: append([]serviceInfo(nil), providers...),
	}
	if err := r.switchProvider(nil); err != nil {
		return nil, err
	}
	return r, nil
}

type discoveryLayerReaderAt struct {
	cfg       config
	digest    string
	bootstrap string
	providers []serviceInfo

	mu       sync.Mutex
	endpoint string
	conn     *grpc.ClientConn
	client   layerstorepb.LayerStoreClient
	size     int64
}

func (r *discoveryLayerReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refreshProvidersLocked()

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

	excluded := make(map[string]struct{})
	for {
		total, err := r.readOnceLocked(p[:maxReadable], off)
		if err == nil || errors.Is(err, io.EOF) {
			if int64(total) < int64(len(p)) {
				return total, io.EOF
			}
			return total, err
		}
		log.Printf("layer read failed, trying failover: digest=%s endpoint=%s err=%v", r.digest, r.endpoint, err)
		excluded[r.endpoint] = struct{}{}
		if switchErr := r.switchProvider(excluded); switchErr != nil {
			if total > 0 {
				return total, err
			}
			return 0, fmt.Errorf("read failed and no failover provider: %w", switchErr)
		}
	}
}

func (r *discoveryLayerReaderAt) refreshProvidersLocked() {
	fresh, err := findLayerServices(r.cfg.discoveryAddr, r.digest, r.cfg.grpcTimeout, r.cfg.grpcInsecure)
	if err != nil {
		return
	}
	if len(fresh) > 0 {
		r.providers = fresh
	}
}

func (r *discoveryLayerReaderAt) readOnceLocked(p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		remaining := len(p) - total
		if remaining > r.cfg.grpcMaxChunk {
			remaining = r.cfg.grpcMaxChunk
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.grpcTimeout)
		resp, err := r.client.ReadLayer(ctx, &layerstorepb.ReadLayerRequest{
			Digest: r.digest,
			Offset: off + int64(total),
			Length: int32(remaining),
		})
		cancel()
		if err != nil {
			return total, err
		}
		n := copy(p[total:], resp.GetData())
		total += n
		if n == 0 || resp.GetEof() {
			break
		}
	}
	if total < len(p) {
		return total, io.EOF
	}
	return total, nil
}

func (r *discoveryLayerReaderAt) switchProvider(excluded map[string]struct{}) error {
	candidates := r.findLayerProviders()
	for _, c := range candidates {
		if _, skip := excluded[c.endpoint]; skip {
			continue
		}
		if err := r.connectProvider(c.endpoint); err != nil {
			log.Printf("provider connect failed: digest=%s endpoint=%s err=%v", r.digest, c.endpoint, err)
			continue
		}
		log.Printf("layer provider selected: digest=%s endpoint=%s", r.digest, c.endpoint)
		return nil
	}
	return fmt.Errorf("no available provider for digest=%s", r.digest)
}

func (r *discoveryLayerReaderAt) findLayerProviders() []serviceInfo {
	candidates := make([]serviceInfo, 0, len(r.providers)+1)
	fresh, err := findLayerServices(r.cfg.discoveryAddr, r.digest, r.cfg.grpcTimeout, r.cfg.grpcInsecure)
	if err == nil && len(fresh) > 0 {
		candidates = append(candidates, fresh...)
	} else {
		candidates = append(candidates, r.providers...)
	}
	if r.bootstrap != "" {
		found := false
		for _, c := range candidates {
			if c.endpoint == r.bootstrap {
				found = true
				break
			}
		}
		if !found {
			candidates = append(candidates, serviceInfo{endpoint: r.bootstrap})
		}
	}
	return rankServicesForAffinity(candidates, r.cfg.nodeID)
}

func findLayerServices(discoveryAddr, digest string, timeout time.Duration, insecureTransport bool) ([]serviceInfo, error) {
	conn, err := dialGRPC(discoveryAddr, timeout, insecureTransport)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := discoverypb.NewServiceDiscoveryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := client.FindImage(ctx, &discoverypb.FindImageRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]serviceInfo, 0, len(resp.GetServices()))
	for _, svc := range resp.GetServices() {
		if svc == nil {
			continue
		}
		for _, d := range svc.GetLayerDigests() {
			if d == digest {
				out = append(out, serviceInfo{
					nodeID:   strings.TrimSpace(svc.GetNodeId()),
					endpoint: strings.TrimSpace(svc.GetEndpoint()),
				})
				break
			}
		}
	}
	return dedupeServiceInfos(out), nil
}

func (r *discoveryLayerReaderAt) connectProvider(endpoint string) error {
	conn, err := dialGRPC(endpoint, r.cfg.grpcTimeout, r.cfg.grpcInsecure)
	if err != nil {
		return err
	}
	client := layerstorepb.NewLayerStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.grpcTimeout)
	defer cancel()
	statResp, err := client.StatLayer(ctx, &layerstorepb.StatLayerRequest{Digest: r.digest})
	if err != nil {
		_ = conn.Close()
		return err
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
	r.conn = conn
	r.client = client
	r.endpoint = endpoint
	r.size = statResp.GetAfsSize()
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
		return [][]string{{"fusermount3", "-u", mountpoint}, {"fusermount", "-u", mountpoint}, {"umount", mountpoint}}
	case "darwin":
		return [][]string{{"umount", mountpoint}, {"diskutil", "unmount", "force", mountpoint}}
	default:
		return [][]string{{"umount", mountpoint}}
	}
}

func mountLayerReader(reader *layerformat.Reader, mountpoint, source string, debug bool, tempDir string) (*fuse.Server, error) {
	root := layerfuse.NewRootWithTempDir(reader, tempDir)
	server, err := fusefs.Mount(mountpoint, root, &fusefs.Options{MountOptions: fuse.MountOptions{Debug: debug, FsName: fmt.Sprintf("afslyr:%s", source), Name: "afslyr", Options: []string{"ro", "exec"}}})
	if err != nil {
		if strings.Contains(err.Error(), "no FUSE mount utility found") {
			return nil, fmt.Errorf("mount fuse: %w (hint: install FUSE runtime)", err)
		}
		return nil, fmt.Errorf("mount fuse: %w", err)
	}
	return server, nil
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
	}, "|")
}
