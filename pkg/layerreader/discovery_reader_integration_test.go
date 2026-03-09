//go:build integration

package layerreader

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/discovery"
	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerstore"
	"github.com/reyoung/afs/pkg/layerstorepb"
	"github.com/reyoung/afs/pkg/registry"
)

func TestIntegrationDiscoveryBackedReaderAtConcurrentReadPerfAndMD5(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	image := strings.TrimSpace(os.Getenv("AFS_INTEGRATION_IMAGE"))
	if image == "" {
		image = "ubuntu"
	}
	tag := strings.TrimSpace(os.Getenv("AFS_INTEGRATION_TAG"))
	if tag == "" {
		tag = "latest"
	}
	workers := envIntOrDefault("AFS_INTEGRATION_READER_WORKERS", 8)
	if workers < 2 {
		workers = 2
	}
	iterations := envIntOrDefault("AFS_INTEGRATION_READER_ITERATIONS", 8)
	if iterations < 1 {
		iterations = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	ref, err := registry.ParseImageReference(image, tag)
	if err != nil {
		t.Fatalf("parse image reference: %v", err)
	}
	t.Logf("integration image resolved: image=%s tag=%s registry=%s repository=%s reference=%s", image, tag, ref.Registry, ref.Repository, ref.Reference)

	discAddr, stopDiscovery := startDiscoveryServer(t)
	defer stopDiscovery()

	cacheDir := filepath.Join(t.TempDir(), "cache")
	lsAddr, stopLayerstore := startLayerstoreServer(t, cacheDir, integrationAuthConfigs(t, ref.Registry), map[string][]string{
		ref.Registry: localDockerMirrorsForRegistry(t, ref.Registry),
	})
	defer stopLayerstore()

	pullResp := pullImage(t, ctx, lsAddr, image, tag)
	if len(pullResp.GetLayers()) == 0 {
		t.Fatalf("no layers pulled for image=%s tag=%s", image, tag)
	}
	target := pickLargestLayer(pullResp)
	t.Logf("selected layer for read perf: digest=%s afs_size=%d", target.GetDigest(), target.GetAfsSize())

	imageKeyVal := imageKey(image, tag, "linux", "amd64", "")
	heartbeat(t, ctx, discAddr, "node-1", lsAddr, imageKeyVal, digestsFromPull(pullResp))

	reader, err := NewDiscoveryBackedLayerReader(Config{
		DiscoveryAddr: discAddr,
		GRPCTimeout:   10 * time.Second,
		GRPCMaxChunk:  4 << 20,
		GRPCInsecure:  true,
		NodeID:        "node-1",
	}, target.GetDigest(), lsAddr, []ServiceInfo{{NodeID: "node-1", Endpoint: lsAddr}})
	if err != nil {
		t.Fatalf("new discovery backed reader: %v", err)
	}
	remoteTimed := &timedReaderAt{ra: reader}
	remoteLayerReader, err := layerformat.NewReader(remoteTimed)
	if err != nil {
		t.Fatalf("open remote layer reader: %v", err)
	}

	layerPath := layerCachePath(cacheDir, target.GetDigest())
	localFd, err := os.Open(layerPath)
	if err != nil {
		t.Fatalf("open local cached layer: %v", err)
	}
	defer localFd.Close()
	localTimed := &timedReaderAt{ra: localFd}
	localLayerReader, err := layerformat.NewReader(localTimed)
	if err != nil {
		t.Fatalf("open local layer reader: %v", err)
	}

	selected := pickEntriesForPerf(localLayerReader.Entries(), workers)
	if len(selected) < 2 {
		t.Fatalf("not enough regular files for perf test in layer=%s", target.GetDigest())
	}
	t.Logf("selected files=%d workers=%d iterations=%d", len(selected), workers, iterations)

	expectedMD5 := make(map[string]string, len(selected))
	for _, e := range selected {
		md5Hex, size, hashErr := computeFileMD5(localLayerReader, e.Path)
		if hashErr != nil {
			t.Fatalf("compute expected md5 for %s: %v", e.Path, hashErr)
		}
		if size <= 0 {
			t.Fatalf("selected file has empty payload: %s", e.Path)
		}
		expectedMD5[e.Path] = md5Hex
	}

	type workerResult struct {
		path       string
		bytesTotal int64
		timeTotal  time.Duration
	}
	runPhase := func(reader *layerformat.Reader, timed *timedReaderAt, phase string) (allBytes int64, elapsed time.Duration, peakBps float64, peakPath string, rs readAtStats, err error) {
		startStats := timed.snapshot()
		results := make([]workerResult, len(selected))
		errCh := make(chan error, len(selected))

		startCh := make(chan struct{})
		var wg sync.WaitGroup
		for i := range selected {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				entry := selected[i]
				var bytesTotal int64
				var timeTotal time.Duration
				for j := 0; j < iterations; j++ {
					begin := time.Now()
					md5Hex, size, readErr := computeFileMD5(reader, entry.Path)
					timeTotal += time.Since(begin)
					if readErr != nil {
						errCh <- fmt.Errorf("%s worker path=%s iteration=%d read failed: %w", phase, entry.Path, j, readErr)
						return
					}
					if md5Hex != expectedMD5[entry.Path] {
						errCh <- fmt.Errorf("%s worker path=%s iteration=%d md5 mismatch got=%s want=%s", phase, entry.Path, j, md5Hex, expectedMD5[entry.Path])
						return
					}
					bytesTotal += size
				}
				results[i] = workerResult{
					path:       entry.Path,
					bytesTotal: bytesTotal,
					timeTotal:  timeTotal,
				}
			}()
		}

		runStart := time.Now()
		close(startCh)
		wg.Wait()
		close(errCh)
		for e := range errCh {
			return 0, 0, 0, "", readAtStats{}, e
		}
		runElapsed := time.Since(runStart)

		var totalBytes int64
		workerPeakBps := 0.0
		workerPeakPath := ""
		for _, r := range results {
			totalBytes += r.bytesTotal
			if r.timeTotal <= 0 {
				continue
			}
			bps := float64(r.bytesTotal) / r.timeTotal.Seconds()
			if bps > workerPeakBps {
				workerPeakBps = bps
				workerPeakPath = r.path
			}
		}
		if totalBytes <= 0 {
			return 0, 0, 0, "", readAtStats{}, fmt.Errorf("%s no bytes were read in perf test", phase)
		}
		endStats := timed.snapshot()
		return totalBytes, runElapsed, workerPeakBps, workerPeakPath, endStats.delta(startStats), nil
	}

	remoteBytes, remoteElapsed, remotePeakBps, remotePeakPath, remoteStats, err := runPhase(remoteLayerReader, remoteTimed, "remote")
	if err != nil {
		t.Fatal(err)
	}
	localBytes, localElapsed, localPeakBps, localPeakPath, localStats, err := runPhase(localLayerReader, localTimed, "local")
	if err != nil {
		t.Fatal(err)
	}
	remoteAggBps := float64(remoteBytes) / remoteElapsed.Seconds()
	localAggBps := float64(localBytes) / localElapsed.Seconds()
	overhead := 0.0
	if localAggBps > 0 {
		overhead = (localAggBps - remoteAggBps) / localAggBps * 100
	}
	t.Logf("perf remote image=%s:%s layer=%s workers=%d iterations=%d total_bytes=%d elapsed=%s agg_throughput=%.2f MiB/s peak_worker_throughput=%.2f MiB/s peak_path=%s",
		image, tag, target.GetDigest(), len(selected), iterations, remoteBytes, remoteElapsed,
		remoteAggBps/(1024*1024), remotePeakBps/(1024*1024), remotePeakPath)
	t.Logf("perf local  image=%s:%s layer=%s workers=%d iterations=%d total_bytes=%d elapsed=%s agg_throughput=%.2f MiB/s peak_worker_throughput=%.2f MiB/s peak_path=%s",
		image, tag, target.GetDigest(), len(selected), iterations, localBytes, localElapsed,
		localAggBps/(1024*1024), localPeakBps/(1024*1024), localPeakPath)
	t.Logf("perf overhead remote_vs_local=%.2f%%", overhead)
	logReadStats(t, "remote", remoteStats)
	logReadStats(t, "local", localStats)
	remoteNsPerMiB := nsPerMiB(remoteStats.gotBytes, remoteStats.durationNanos)
	localNsPerMiB := nsPerMiB(localStats.gotBytes, localStats.durationNanos)
	ratio := 0.0
	if localNsPerMiB > 0 {
		ratio = remoteNsPerMiB / localNsPerMiB
	}
	t.Logf("readat ns_per_mib remote=%.2f local=%.2f ratio=%.2fx", remoteNsPerMiB, localNsPerMiB, ratio)
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

func startLayerstoreServer(t *testing.T, cacheDir string, authConfigs []layerstore.RegistryAuthConfig, mirrors map[string][]string) (addr string, stop func()) {
	t.Helper()
	svc, err := layerstore.NewService(cacheDir, authConfigs)
	if err != nil {
		t.Fatalf("new layerstore service: %v", err)
	}
	if len(mirrors) > 0 {
		svc.SetRegistryMirrors(mirrors)
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

func integrationAuthConfigs(t *testing.T, registryHost string) []layerstore.RegistryAuthConfig {
	t.Helper()
	b64 := strings.TrimSpace(os.Getenv("AFS_INTEGRATION_DOCKER_AUTH_B64"))
	if b64 == "" {
		b64 = dockerConfigAuthForRegistry(t, registryHost)
	}
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
		RegistryHost: registryHost,
		Username:     userPass[0],
		Password:     userPass[1],
	}}
}

func dockerConfigAuthForRegistry(t *testing.T, registryHost string) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Logf("skip local docker auth lookup: user home dir error: %v", err)
		return ""
	}
	cfgPath := filepath.Join(home, ".docker", "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Logf("skip local docker auth lookup: read %s failed: %v", cfgPath, err)
		return ""
	}
	var cfg struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Logf("skip local docker auth lookup: decode %s failed: %v", cfgPath, err)
		return ""
	}
	target := strings.TrimSpace(strings.ToLower(registryHost))
	for key, v := range cfg.Auths {
		auth := strings.TrimSpace(v.Auth)
		if auth == "" {
			continue
		}
		if dockerAuthKeyMatchesRegistry(key, target) {
			t.Logf("using docker config auth key=%s for registry=%s", key, target)
			return auth
		}
	}
	t.Logf("no matching docker config auth for registry=%s", target)
	return ""
}

func dockerAuthKeyMatchesRegistry(key, registryHost string) bool {
	key = strings.TrimSpace(strings.ToLower(key))
	if key == "" || registryHost == "" {
		return false
	}
	if strings.Contains(key, "://") {
		if u, err := urlParseHost(key); err == nil {
			key = strings.ToLower(strings.TrimSpace(u))
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

func urlParseHost(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("empty host")
	}
	return u.Host, nil
}

func localDockerMirrorsForRegistry(t *testing.T, registryHost string) []string {
	t.Helper()
	out, err := exec.Command("docker", "info", "--format", "{{json .RegistryConfig.Mirrors}}").CombinedOutput()
	if err != nil {
		t.Logf("skip local docker mirror lookup: docker info failed: %v (%s)", err, strings.TrimSpace(string(out)))
		return nil
	}
	var mirrors []string
	if err := json.Unmarshal(out, &mirrors); err != nil {
		t.Logf("skip local docker mirror lookup: parse output failed: %v (%s)", err, strings.TrimSpace(string(out)))
		return nil
	}
	seen := make(map[string]struct{})
	cleaned := make([]string, 0, len(mirrors))
	for _, m := range mirrors {
		host, err := urlParseHost(m)
		if err != nil {
			continue
		}
		host = strings.TrimSpace(strings.ToLower(host))
		if host == "" || host == strings.ToLower(strings.TrimSpace(registryHost)) {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		cleaned = append(cleaned, host)
	}
	if len(cleaned) > 0 {
		t.Logf("using docker daemon mirrors for registry=%s: %v", registryHost, cleaned)
	}
	return cleaned
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
	callCtx, callCancel := context.WithTimeout(ctx, 20*time.Minute)
	defer callCancel()
	resp, err := client.PullImage(callCtx, &layerstorepb.PullImageRequest{
		Image:           image,
		Tag:             tag,
		PlatformOs:      "linux",
		PlatformArch:    "amd64",
		ForceLocalFetch: true,
	})
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "Too Many Requests") ||
			strings.Contains(msg, "toomanyrequests") ||
			strings.Contains(msg, "Forbidden") ||
			strings.Contains(msg, "denied") ||
			strings.Contains(msg, "unauthorized") {
			t.Skipf("pull image skipped due to registry access limit/auth issue (%s:%s): %v", image, tag, err)
		}
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

func pickLargestLayer(resp *layerstorepb.PullImageResponse) *layerstorepb.Layer {
	var best *layerstorepb.Layer
	for _, l := range resp.GetLayers() {
		if best == nil || l.GetAfsSize() > best.GetAfsSize() {
			best = l
		}
	}
	return best
}

func pickEntriesForPerf(entries []layerformat.Entry, maxN int) []layerformat.Entry {
	candidates := make([]layerformat.Entry, 0, len(entries))
	for _, e := range entries {
		if e.Type != layerformat.EntryTypeFile || strings.TrimSpace(e.Path) == "" {
			continue
		}
		if e.CompressedSize <= 0 || e.UncompressedSize <= 0 {
			continue
		}
		candidates = append(candidates, e)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].UncompressedSize == candidates[j].UncompressedSize {
			return candidates[i].Path < candidates[j].Path
		}
		return candidates[i].UncompressedSize > candidates[j].UncompressedSize
	})
	if len(candidates) > maxN {
		return candidates[:maxN]
	}
	return candidates
}

func computeFileMD5(r *layerformat.Reader, path string) (hexMD5 string, bytes int64, err error) {
	h := md5.New()
	n, err := r.CopyFile(path, h)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func layerCachePath(cacheDir, digest string) string {
	parts := strings.SplitN(strings.TrimSpace(digest), ":", 2)
	if len(parts) != 2 {
		return ""
	}
	return filepath.Join(cacheDir, "layers", parts[0], strings.ToLower(parts[1])+".afslyr")
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

func digestsFromPull(resp *layerstorepb.PullImageResponse) []string {
	out := make([]string, 0, len(resp.GetLayers()))
	for _, l := range resp.GetLayers() {
		out = append(out, l.GetDigest())
	}
	return out
}

func envIntOrDefault(name string, defaultVal int) int {
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

type timedReaderAt struct {
	ra io.ReaderAt

	calls    atomic.Int64
	reqBytes atomic.Int64
	gotBytes atomic.Int64
	durNanos atomic.Int64
}

type readAtStats struct {
	calls          int64
	requestedBytes int64
	gotBytes       int64
	durationNanos  int64
}

func (t *timedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	begin := time.Now()
	n, err := t.ra.ReadAt(p, off)
	elapsed := time.Since(begin).Nanoseconds()
	t.calls.Add(1)
	t.reqBytes.Add(int64(len(p)))
	t.gotBytes.Add(int64(n))
	t.durNanos.Add(elapsed)
	return n, err
}

func (t *timedReaderAt) snapshot() readAtStats {
	return readAtStats{
		calls:          t.calls.Load(),
		requestedBytes: t.reqBytes.Load(),
		gotBytes:       t.gotBytes.Load(),
		durationNanos:  t.durNanos.Load(),
	}
}

func (s readAtStats) delta(prev readAtStats) readAtStats {
	return readAtStats{
		calls:          s.calls - prev.calls,
		requestedBytes: s.requestedBytes - prev.requestedBytes,
		gotBytes:       s.gotBytes - prev.gotBytes,
		durationNanos:  s.durationNanos - prev.durationNanos,
	}
}

func logReadStats(t *testing.T, phase string, s readAtStats) {
	avgReq := 0.0
	avgGot := 0.0
	if s.calls > 0 {
		avgReq = float64(s.requestedBytes) / float64(s.calls)
		avgGot = float64(s.gotBytes) / float64(s.calls)
	}
	t.Logf("readat %s calls=%d req_bytes=%d got_bytes=%d total_readat_time=%s avg_req=%.1f avg_got=%.1f",
		phase, s.calls, s.requestedBytes, s.gotBytes, time.Duration(s.durationNanos), avgReq, avgGot)
}

func nsPerMiB(bytes, nanos int64) float64 {
	if bytes <= 0 {
		return 0
	}
	miB := float64(bytes) / (1024 * 1024)
	if miB <= 0 {
		return 0
	}
	return float64(nanos) / miB
}
