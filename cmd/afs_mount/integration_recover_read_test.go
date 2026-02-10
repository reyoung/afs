//go:build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/discovery"
	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerstore"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

func TestIntegrationMountReadRecoversAfterLayerFileDeleted(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root (mount/fuse-overlayfs)")
	}
	requireToolsOrSkip(t, "fuse-overlayfs", "fusermount3", "mount", "umount")

	image := strings.TrimSpace(os.Getenv("AFS_INTEGRATION_IMAGE"))
	if image == "" {
		image = "registry.k8s.io/kube-apiserver"
	}
	tag := strings.TrimSpace(os.Getenv("AFS_INTEGRATION_TAG"))
	if tag == "" {
		tag = "v1.30.0"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	authConfigs := integrationAuthConfigs(t)

	discAddr, stopDiscovery := startDiscoveryServer(t)
	defer stopDiscovery()

	cache1 := filepath.Join(t.TempDir(), "cache-1")
	cache2 := filepath.Join(t.TempDir(), "cache-2")
	lsAddr1, stopLS1 := startLayerstoreServer(t, cache1, authConfigs)
	defer stopLS1()
	lsAddr2, stopLS2 := startLayerstoreServer(t, cache2, authConfigs)
	defer stopLS2()

	pull1 := pullImage(t, ctx, lsAddr1, image, tag)
	if len(pull1.GetLayers()) == 0 {
		t.Fatalf("no layers pulled for image=%s tag=%s", image, tag)
	}
	if err := copyDir(cache1, cache2); err != nil {
		t.Fatalf("copy cache from layerstore1 to layerstore2: %v", err)
	}
	targetDigest := pull1.GetLayers()[len(pull1.GetLayers())-1].GetDigest()
	targetLayerPath := layerCachePath(cache1, targetDigest)
	targetEntryPath, expected := pickReadableFileFromLayer(t, targetLayerPath)
	t.Logf("target digest=%s file=%s", targetDigest, targetEntryPath)

	imageKeyVal := imageKey(image, tag, "linux", "amd64", "")
	layerDigests := digestsFromPull(pull1)
	heartbeat(t, ctx, discAddr, "node-1", lsAddr1, imageKeyVal, layerDigests)
	heartbeat(t, ctx, discAddr, "node-2", lsAddr2, imageKeyVal, layerDigests)

	mountpoint := filepath.Join(t.TempDir(), "mnt")
	workDir := filepath.Join(t.TempDir(), "work")
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir workdir: %v", err)
	}

	mountBin := filepath.Join(t.TempDir(), "afs_mount.testbin")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", mountBin, ".")
	buildCmd.Dir = "."
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build afs_mount: %v\n%s", err, out)
	}

	mountCtx, mountCancel := context.WithCancel(ctx)
	defer mountCancel()
	mountCmd := exec.CommandContext(
		mountCtx,
		mountBin,
		"-mountpoint", mountpoint,
		"-work-dir", workDir,
		"-mount-proc-dev=false",
		"-discovery-addr", discAddr,
		"-image", image,
		"-tag", tag,
		"-platform-os", "linux",
		"-platform-arch", "amd64",
		"-pull-timeout", "10m",
	)
	logBuf := &safeBuffer{}
	stdout, err := mountCmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderr, err := mountCmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	selectedEPCh := make(chan string, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go scanMountLogs(&wg, stdout, targetDigest, selectedEPCh, logBuf)
	go scanMountLogs(&wg, stderr, targetDigest, selectedEPCh, logBuf)
	if err := mountCmd.Start(); err != nil {
		t.Fatalf("start afs_mount: %v", err)
	}
	defer func() {
		if mountCmd.Process != nil {
			_ = mountCmd.Process.Signal(syscall.SIGTERM)
		}
		mountCancel()
		_ = mountCmd.Wait()
		wg.Wait()
		_ = forceUnmountForTest("linux", mountpoint)
		_ = forceUnmountLayerDirs(workDir)
	}()

	targetMountPath := filepath.Join(mountpoint, filepath.FromSlash(targetEntryPath))
	waitForPath(t, targetMountPath, 2*time.Minute, logBuf)
	selectedEP := waitForEndpoint(t, selectedEPCh, 2*time.Minute, logBuf)

	deleteCache := map[string]string{lsAddr1: cache1, lsAddr2: cache2}[selectedEP]
	if deleteCache == "" {
		t.Fatalf("selected endpoint %s is unknown; logs:\n%s", selectedEP, logBuf.String())
	}
	deletePath := layerCachePath(deleteCache, targetDigest)
	if err := os.Remove(deletePath); err != nil {
		t.Fatalf("remove target layer cache file %s: %v", deletePath, err)
	}

	got, err := os.ReadFile(targetMountPath)
	if err != nil {
		t.Fatalf("read file after deleting layer cache: %v\nlogs:\n%s", err, logBuf.String())
	}
	if !bytes.Equal(got, expected) {
		t.Fatalf("read content mismatch after recovery: got=%d want=%d", len(got), len(expected))
	}
}

func startDiscoveryServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen discovery: %v", err)
	}
	s := grpc.NewServer()
	discoverypb.RegisterServiceDiscoveryServer(s, discovery.NewService())
	go func() { _ = s.Serve(lis) }()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

func startLayerstoreServer(t *testing.T, cacheDir string, authConfigs []layerstore.RegistryAuthConfig) (addr string, stop func()) {
	t.Helper()
	svc, err := layerstore.NewService(cacheDir, authConfigs)
	if err != nil {
		t.Fatalf("new layerstore service: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen layerstore: %v", err)
	}
	s := grpc.NewServer()
	layerstorepb.RegisterLayerStoreServer(s, svc)
	go func() { _ = s.Serve(lis) }()
	return lis.Addr().String(), func() {
		s.Stop()
		_ = lis.Close()
	}
}

func integrationAuthConfigs(t *testing.T) []layerstore.RegistryAuthConfig {
	t.Helper()
	b64 := strings.TrimSpace(os.Getenv("AFS_INTEGRATION_DOCKER_AUTH_B64"))
	if b64 == "" {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode AFS_INTEGRATION_DOCKER_AUTH_B64: %v", err)
	}
	userPass := strings.SplitN(string(raw), ":", 2)
	if len(userPass) != 2 || strings.TrimSpace(userPass[0]) == "" {
		t.Fatalf("invalid docker auth payload in AFS_INTEGRATION_DOCKER_AUTH_B64")
	}
	return []layerstore.RegistryAuthConfig{{
		RegistryHost: "registry-1.docker.io",
		Username:     userPass[0],
		Password:     userPass[1],
	}}
}

func pullImage(t *testing.T, ctx context.Context, endpoint, image, tag string) *layerstorepb.PullImageResponse {
	t.Helper()
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial layerstore %s: %v", endpoint, err)
	}
	defer conn.Close()
	client := layerstorepb.NewLayerStoreClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, 12*time.Minute)
	defer callCancel()
	resp, err := client.PullImage(callCtx, &layerstorepb.PullImageRequest{
		Image:        image,
		Tag:          tag,
		PlatformOs:   "linux",
		PlatformArch: "amd64",
		Force:        false,
	})
	if err != nil {
		t.Fatalf("pull image from %s (%s:%s): %v", endpoint, image, tag, err)
	}
	return resp
}

func heartbeat(t *testing.T, ctx context.Context, discoveryAddr, nodeID, endpoint, imageKeyVal string, layerDigests []string) {
	t.Helper()
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, discoveryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial discovery %s: %v", discoveryAddr, err)
	}
	defer conn.Close()
	client := discoverypb.NewServiceDiscoveryClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	if _, err := client.Heartbeat(callCtx, &discoverypb.HeartbeatRequest{
		NodeId:       nodeID,
		Endpoint:     endpoint,
		LayerDigests: layerDigests,
		CachedImages: []string{imageKeyVal},
	}); err != nil {
		t.Fatalf("heartbeat %s -> %s: %v", nodeID, endpoint, err)
	}
}

func digestsFromPull(resp *layerstorepb.PullImageResponse) []string {
	out := make([]string, 0, len(resp.GetLayers()))
	for _, l := range resp.GetLayers() {
		out = append(out, l.GetDigest())
	}
	return out
}

func layerCachePath(cacheDir, digest string) string {
	parts := strings.SplitN(strings.TrimSpace(digest), ":", 2)
	return filepath.Join(cacheDir, "layers", parts[0], parts[1]+".afslyr")
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}

func pickReadableFileFromLayer(t *testing.T, layerPath string) (path string, expected []byte) {
	t.Helper()
	f, err := os.Open(layerPath)
	if err != nil {
		t.Fatalf("open layer file %s: %v", layerPath, err)
	}
	defer f.Close()

	r, err := layerformat.NewReader(f)
	if err != nil {
		t.Fatalf("open layer reader %s: %v", layerPath, err)
	}
	entries := r.Entries()
	for _, e := range entries {
		if e.Type != layerformat.EntryTypeFile || e.UncompressedSize <= 0 || e.UncompressedSize > 512*1024 {
			continue
		}
		if strings.HasPrefix(filepath.Base(e.Path), ".wh.") || strings.Contains(e.Path, "/.wh.") {
			continue
		}
		b, err := r.ReadFile(e.Path)
		if err == nil && len(b) > 0 {
			return e.Path, b
		}
	}
	t.Fatalf("cannot find readable regular file in layer %s", layerPath)
	return "", nil
}

func waitForPath(t *testing.T, path string, timeout time.Duration, logs *safeBuffer) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting file %s; logs:\n%s", path, logs.String())
}

func waitForEndpoint(t *testing.T, ch <-chan string, timeout time.Duration, logs *safeBuffer) string {
	t.Helper()
	select {
	case ep := <-ch:
		return ep
	case <-time.After(timeout):
		t.Fatalf("timeout waiting selected endpoint from logs:\n%s", logs.String())
		return ""
	}
}

func requireToolsOrSkip(t *testing.T, bins ...string) {
	t.Helper()
	for _, b := range bins {
		if _, err := exec.LookPath(b); err != nil {
			t.Skipf("tool %s not found: %v", b, err)
		}
	}
}

func scanMountLogs(wg *sync.WaitGroup, r io.Reader, targetDigest string, selectedEPCh chan<- string, out *safeBuffer) {
	defer wg.Done()
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := s.Text()
		out.AppendLine(line)
		prefix := "layer provider selected: digest=" + targetDigest + " endpoint="
		if i := strings.Index(line, prefix); i >= 0 {
			ep := strings.TrimSpace(line[i+len(prefix):])
			select {
			case selectedEPCh <- ep:
			default:
			}
		}
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) AppendLine(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, _ = b.buf.WriteString(s)
	_ = b.buf.WriteByte('\n')
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func forceUnmountForTest(goos string, mountpoint string) error {
	for _, argv := range unmountCandidates(goos, mountpoint) {
		if len(argv) == 0 {
			continue
		}
		cmd := exec.Command(argv[0], argv[1:]...)
		if err := cmd.Run(); err == nil {
			return nil
		}
	}
	return fmt.Errorf("failed to unmount %s", mountpoint)
}

func forceUnmountLayerDirs(workDir string) error {
	root := filepath.Join(workDir, "layers")
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		layerMount := filepath.Join(root, ent.Name())
		_ = forceUnmountForTest("linux", layerMount)
	}
	return nil
}
