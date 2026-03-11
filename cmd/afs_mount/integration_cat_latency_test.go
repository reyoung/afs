//go:build integration

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/layerstore"
)

func TestIntegrationMountCatFileLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root (mount/fuse-overlayfs)")
	}
	requireToolsOrSkip(t, "fuse-overlayfs", "fusermount3", "mount", "umount", "cat")

	image, tag := integrationImageTag()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	cacheDir := filepath.Join(t.TempDir(), "cache")
	authConfigs := integrationAuthConfigsForImage(t, image)
	mirrors := integrationRegistryMirrorsForImage(t, image)
	lsAddr, stopLS := startLayerstoreServer(t, cacheDir, authConfigs, mirrors)
	defer stopLS()

	discAddr, stopDiscovery := startDiscoveryServer(t, toDiscoveryAuthConfigs(authConfigs), mirrors)
	defer stopDiscovery()

	pullResp := pullImage(t, ctx, discAddr, lsAddr, image, tag)
	if len(pullResp.GetLayers()) == 0 {
		t.Fatalf("no layers pulled for image=%s tag=%s", image, tag)
	}
	target := pullResp.GetLayers()[len(pullResp.GetLayers())-1]
	targetLayerPath := layerCachePath(cacheDir, target.GetDigest())
	targetEntryPath, expected := pickReadableFileFromLayer(t, targetLayerPath)

	heartbeat(t, ctx, discAddr, "node-1", lsAddr, digestsFromPull(pullResp))

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

	var wg sync.WaitGroup
	wg.Add(2)
	go scanMountLogs(&wg, stdout, "", make(chan string, 1), logBuf)
	go scanMountLogs(&wg, stderr, "", make(chan string, 1), logBuf)

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

	// Measure first-read cat latency on the mounted file.
	catCtx, catCancel := context.WithTimeout(ctx, 30*time.Second)
	defer catCancel()
	begin := time.Now()
	out, err := exec.CommandContext(catCtx, "cat", targetMountPath).Output()
	latency := time.Since(begin)
	if err != nil {
		t.Fatalf("cat %s failed: %v\nlogs:\n%s", targetMountPath, err, logBuf.String())
	}
	if string(out) != string(expected) {
		t.Fatalf("cat output mismatch: got=%d want=%d", len(out), len(expected))
	}
	if latency <= 0 {
		t.Fatalf("invalid cat latency: %s", latency)
	}
	t.Logf("cat latency file=%s size=%d latency=%s", targetEntryPath, len(out), latency)
}

func TestIntegrationMountCatFileLatencyPercentiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root (mount/fuse-overlayfs)")
	}
	requireToolsOrSkip(t, "fuse-overlayfs", "fusermount3", "mount", "umount", "cat")

	image, tag := integrationImageTag()
	targetFile := strings.TrimSpace(os.Getenv("AFS_INTEGRATION_TARGET_FILE"))
	if targetFile == "" {
		targetFile = "/bin/true"
	}
	catTimes := envIntOrDefaultLocal("AFS_INTEGRATION_CAT_TIMES", 50)
	if catTimes < 1 {
		catTimes = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	cacheDir := filepath.Join(t.TempDir(), "cache")
	authConfigs := integrationAuthConfigsForImage(t, image)
	mirrors := integrationRegistryMirrorsForImage(t, image)
	lsAddr, stopLS := startLayerstoreServer(t, cacheDir, authConfigs, mirrors)
	defer stopLS()

	discAddr, stopDiscovery := startDiscoveryServer(t, toDiscoveryAuthConfigs(authConfigs), mirrors)
	defer stopDiscovery()

	pullResp := pullImage(t, ctx, discAddr, lsAddr, image, tag)
	if len(pullResp.GetLayers()) == 0 {
		t.Fatalf("no layers pulled for image=%s tag=%s", image, tag)
	}

	heartbeat(t, ctx, discAddr, "node-1", lsAddr, digestsFromPull(pullResp))

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

	var wg sync.WaitGroup
	wg.Add(2)
	go scanMountLogs(&wg, stdout, "", make(chan string, 1), logBuf)
	go scanMountLogs(&wg, stderr, "", make(chan string, 1), logBuf)

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

	targetMountPath := filepath.Join(mountpoint, strings.TrimPrefix(filepath.FromSlash(targetFile), "/"))
	waitForPath(t, targetMountPath, 2*time.Minute, logBuf)

	latencies := make([]time.Duration, 0, catTimes)
	for i := 0; i < catTimes; i++ {
		catCtx, catCancel := context.WithTimeout(ctx, 30*time.Second)
		begin := time.Now()
		out, err := exec.CommandContext(catCtx, "cat", targetMountPath).Output()
		latency := time.Since(begin)
		catCancel()
		if err != nil {
			t.Fatalf("cat failed at i=%d path=%s err=%v\nlogs:\n%s", i, targetMountPath, err, logBuf.String())
		}
		if len(out) == 0 {
			t.Fatalf("cat returned empty output at i=%d path=%s", i, targetMountPath)
		}
		latencies = append(latencies, latency)
	}
	p50 := durationPercentile(latencies, 50)
	p95 := durationPercentile(latencies, 95)
	t.Logf("cat latency stats image=%s:%s file=%s n=%d p50=%s p95=%s", image, tag, targetFile, catTimes, p50, p95)
}

func integrationImageTag() (image string, tag string) {
	image = "registry.k8s.io/kube-apiserver"
	tag = "v1.30.0"
	if v := os.Getenv("AFS_INTEGRATION_IMAGE"); v != "" {
		image = v
	}
	if v := os.Getenv("AFS_INTEGRATION_TAG"); v != "" {
		tag = v
	}
	return image, tag
}

func envIntOrDefaultLocal(name string, defaultVal int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return defaultVal
	}
	return v
}

func integrationAuthConfigsForImage(t *testing.T, image string) []layerstore.RegistryAuthConfig {
	t.Helper()
	host := registryHostFromImage(image)
	if host == "" {
		return integrationAuthConfigs(t)
	}

	type dockerCfg struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	cfgPath := filepath.Join(os.Getenv("HOME"), ".docker", "config.json")
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return integrationAuthConfigs(t)
	}
	var cfg dockerCfg
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return integrationAuthConfigs(t)
	}
	auth := dockerConfigAuthForRegistry(host, cfg.Auths)
	if strings.TrimSpace(auth) == "" {
		return integrationAuthConfigs(t)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth))
	if err != nil {
		return integrationAuthConfigs(t)
	}
	userPass := strings.SplitN(string(decoded), ":", 2)
	if len(userPass) != 2 || strings.TrimSpace(userPass[0]) == "" {
		return integrationAuthConfigs(t)
	}
	return []layerstore.RegistryAuthConfig{{
		RegistryHost: host,
		Username:     userPass[0],
		Password:     userPass[1],
	}}
}

func dockerConfigAuthForRegistry(host string, auths map[string]struct {
	Auth string `json:"auth"`
}) string {
	target := strings.TrimSpace(strings.ToLower(host))
	bestScore := 0
	bestAuth := ""
	for key, entry := range auths {
		auth := strings.TrimSpace(entry.Auth)
		if auth == "" {
			continue
		}
		score := dockerAuthMatchScore(key, target)
		if score > bestScore {
			bestScore = score
			bestAuth = auth
		}
	}
	return bestAuth
}

func dockerAuthKeyMatchesRegistry(key, registryHost string) bool {
	key = strings.TrimSpace(strings.ToLower(key))
	if key == "" || registryHost == "" {
		return false
	}
	if strings.Contains(key, "://") {
		if u, err := url.Parse(key); err == nil {
			key = strings.ToLower(strings.TrimSpace(u.Host))
		}
	}
	key = strings.TrimPrefix(key, "https://")
	key = strings.TrimPrefix(key, "http://")
	key = strings.TrimSuffix(key, "/")
	switch key {
	case "index.docker.io", "index.docker.io/v1", "index.docker.io/v1/access-token", "index.docker.io/v1/refresh-token":
		key = "registry-1.docker.io"
	}
	return key == registryHost
}

func dockerAuthMatchScore(key, registryHost string) int {
	if !dockerAuthKeyMatchesRegistry(key, registryHost) {
		return 0
	}
	key = strings.TrimSpace(strings.ToLower(key))
	switch {
	case strings.Contains(key, "/access-token"):
		return 10
	case strings.Contains(key, "/refresh-token"):
		return 5
	case strings.Contains(key, "index.docker.io/v1") || strings.Contains(key, "registry-1.docker.io"):
		return 30
	default:
		return 20
	}
}

func registryHostFromImage(image string) string {
	ref := strings.TrimSpace(image)
	if ref == "" {
		return ""
	}
	first := ref
	if i := strings.Index(ref, "/"); i >= 0 {
		first = ref[:i]
	}
	// Docker Hub short name like "ubuntu"
	if !strings.Contains(first, ".") && !strings.Contains(first, ":") && first != "localhost" {
		return "registry-1.docker.io"
	}
	return first
}

func integrationRegistryMirrorsForImage(t *testing.T, image string) map[string][]string {
	t.Helper()
	host := registryHostFromImage(image)
	if host == "" {
		return nil
	}
	mirrors := localDockerMirrorsForRegistry(host)
	if len(mirrors) == 0 {
		return nil
	}
	return map[string][]string{host: mirrors}
}

func localDockerMirrorsForRegistry(registryHost string) []string {
	out, err := exec.Command("docker", "info", "--format", "{{json .RegistryConfig.Mirrors}}").CombinedOutput()
	if err != nil {
		return nil
	}
	var mirrors []string
	if err := json.Unmarshal(out, &mirrors); err != nil {
		return nil
	}
	seen := make(map[string]struct{})
	cleaned := make([]string, 0, len(mirrors))
	target := strings.TrimSpace(strings.ToLower(registryHost))
	for _, m := range mirrors {
		u, err := url.Parse(strings.TrimSpace(m))
		if err != nil || strings.TrimSpace(u.Host) == "" {
			continue
		}
		host := strings.TrimSpace(strings.ToLower(u.Host))
		if host == "" || host == target {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		cleaned = append(cleaned, host)
	}
	return cleaned
}

func durationPercentile(values []time.Duration, p int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		p = 0
	}
	if p >= 100 {
		p = 100
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(p) / 100.0 * float64(len(sorted)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
