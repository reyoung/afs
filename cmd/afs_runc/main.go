package main

import (
	"bufio"
	"encoding/json"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	rootfs       string
	cpuCores     int64
	memoryMB     int64
	timeout      time.Duration
	runcBinary   string
	containerID  string
	user         string
	cwd          string
	env          multiStringFlag
	keepBundle   bool
	noPivot      bool
	noNewKeyring bool
	noCgroupNS   bool
	noPIDNS      bool
	noIPCNS      bool
	noUTSNS      bool
}

type ociSpec struct {
	Version  string     `json:"ociVersion"`
	Root     ociRoot    `json:"root"`
	Process  ociProcess `json:"process"`
	Hostname string     `json:"hostname,omitempty"`
	Linux    ociLinux   `json:"linux"`
	Mounts   []ociMount `json:"mounts,omitempty"`
}

type ociRoot struct {
	Path     string `json:"path"`
	Readonly bool   `json:"readonly"`
}

type ociProcess struct {
	Terminal bool     `json:"terminal"`
	Args     []string `json:"args"`
	Cwd      string   `json:"cwd"`
	Env      []string `json:"env,omitempty"`
	User     ociUser  `json:"user"`
}

type ociLinux struct {
	Resources   ociResources `json:"resources"`
	Namespaces  []ociNS      `json:"namespaces,omitempty"`
	CgroupsPath string       `json:"cgroupsPath,omitempty"`
}

type ociResources struct {
	CPU    ociCPU    `json:"cpu"`
	Memory ociMemory `json:"memory"`
}

type ociCPU struct {
	Period *uint64 `json:"period,omitempty"`
	Quota  *int64  `json:"quota,omitempty"`
}

type ociMemory struct {
	Limit *int64 `json:"limit,omitempty"`
}

type ociNS struct {
	Type string `json:"type"`
}

type ociMount struct {
	Destination string   `json:"destination"`
	Type        string   `json:"type"`
	Source      string   `json:"source"`
	Options     []string `json:"options,omitempty"`
}

type ociUser struct {
	UID            uint32   `json:"uid"`
	GID            uint32   `json:"gid"`
	AdditionalGIDs []uint32 `json:"additionalGids,omitempty"`
}

type passwdEntry struct {
	Name string
	UID  uint32
	GID  uint32
}

type groupEntry struct {
	Name    string
	GID     uint32
	Members []string
}

type multiStringFlag []string

func (f *multiStringFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *multiStringFlag) Set(v string) error {
	*f = append(*f, v)
	return nil
}

func main() {
	cfg, cmdArgs := parseFlags()
	if err := run(cfg, cmdArgs); err != nil {
		log.Fatalf("afs_runc failed: %v", err)
	}
}

func parseFlags() (config, []string) {
	cfg := config{}
	flag.StringVar(&cfg.rootfs, "rootfs", "", "path to rootfs directory")
	flag.Int64Var(&cfg.cpuCores, "cpu", 1, "cpu core limit")
	flag.Int64Var(&cfg.memoryMB, "memory-mb", 256, "memory limit in MB")
	flag.DurationVar(&cfg.timeout, "timeout", time.Second, "max execution time")
	flag.StringVar(&cfg.runcBinary, "runc-binary", "runc", "runc binary path")
	flag.StringVar(&cfg.containerID, "container-id", "", "container id (default: auto generated)")
	flag.StringVar(&cfg.user, "user", "", "container user (name, uid, name:group, or uid:gid)")
	flag.StringVar(&cfg.cwd, "cwd", "/", "working directory inside container")
	flag.Var(&cfg.env, "env", "process environment entry KEY=VALUE (repeatable)")
	flag.BoolVar(&cfg.keepBundle, "keep-bundle", false, "keep generated OCI bundle for debugging")
	flag.BoolVar(&cfg.noPivot, "no-pivot", false, "pass --no-pivot to runc run")
	flag.BoolVar(&cfg.noNewKeyring, "no-new-keyring", false, "pass --no-new-keyring to runc run")
	flag.BoolVar(&cfg.noCgroupNS, "no-cgroup-ns", false, "do not create cgroup namespace in OCI spec")
	flag.BoolVar(&cfg.noPIDNS, "no-pid-ns", false, "do not create pid namespace in OCI spec")
	flag.BoolVar(&cfg.noIPCNS, "no-ipc-ns", false, "do not create ipc namespace in OCI spec")
	flag.BoolVar(&cfg.noUTSNS, "no-uts-ns", false, "do not create uts namespace in OCI spec")
	flag.Parse()

	cmdArgs := flag.Args()
	if strings.TrimSpace(cfg.rootfs) == "" {
		log.Fatal("-rootfs is required")
	}
	if len(cmdArgs) == 0 {
		log.Fatal("command is required, e.g. -- /bin/sh -c 'echo hello'")
	}
	if cfg.cpuCores <= 0 {
		log.Fatal("-cpu must be > 0")
	}
	if cfg.memoryMB <= 0 {
		log.Fatal("-memory-mb must be > 0")
	}
	if cfg.timeout <= 0 {
		log.Fatal("-timeout must be > 0")
	}
	if strings.TrimSpace(cfg.containerID) == "" {
		cfg.containerID = defaultContainerID()
	}
	return cfg, cmdArgs
}

func run(cfg config, cmdArgs []string) error {
	totalStart := time.Now()
	if runtime.GOOS != "linux" {
		return fmt.Errorf("afs_runc only supports linux")
	}
	rootfsAbs, err := filepath.Abs(cfg.rootfs)
	if err != nil {
		return fmt.Errorf("resolve rootfs path: %w", err)
	}
	if err := ensureDir(rootfsAbs); err != nil {
		return fmt.Errorf("invalid rootfs: %w", err)
	}
	if _, err := exec.LookPath(cfg.runcBinary); err != nil {
		return fmt.Errorf("find runc binary %q: %w", cfg.runcBinary, err)
	}

	prepareStart := time.Now()
	bundleDir, cleanup, err := prepareBundle(rootfsAbs, cfg, cmdArgs)
	if err != nil {
		return err
	}
	logTiming("prepare_bundle", time.Since(prepareStart))
	if !cfg.keepBundle {
		defer cleanup()
	}

	runArgs := []string{"run", "--bundle", bundleDir}
	if cfg.noPivot {
		runArgs = append(runArgs, "--no-pivot")
	}
	if cfg.noNewKeyring {
		runArgs = append(runArgs, "--no-new-keyring")
	}
	runArgs = append(runArgs, cfg.containerID)

	runCmd := exec.Command(cfg.runcBinary, runArgs...)
	stdoutPipe, err := runCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open runc stdout pipe: %w", err)
	}
	stderrPipe, err := runCmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("open runc stderr pipe: %w", err)
	}
	runCmd.Stdin = os.Stdin
	startStart := time.Now()
	if err := runCmd.Start(); err != nil {
		return fmt.Errorf("start runc run: %w", err)
	}
	logTiming("runc_start", time.Since(startStart))
	detector := newFirstOutputDetector(startStart, logTimingWithStream)
	var streamWG sync.WaitGroup
	streamWG.Add(2)
	go copyObservedStream("stdout", stdoutPipe, os.Stdout, detector, &streamWG)
	go copyObservedStream("stderr", stderrPipe, os.Stderr, detector, &streamWG)

	waitCh := make(chan error, 1)
	go func() {
		waitErr := runCmd.Wait()
		streamWG.Wait()
		waitCh <- waitErr
	}()
	timedOut := false
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-waitCh:
		waitDur := time.Since(startStart)
		logTiming("runc_wait", waitDur)
		delStart := time.Now()
		deleteContainer(cfg.runcBinary, cfg.containerID)
		logTiming("runc_delete", time.Since(delStart))
		logTiming("total", time.Since(totalStart))
		if err != nil {
			return fmt.Errorf("runc run failed: %w", err)
		}
		return nil
	case sig := <-sigCh:
		log.Printf("received %s, stopping container %s", sig, cfg.containerID)
		if killErr := exec.Command(cfg.runcBinary, "kill", cfg.containerID, "KILL").Run(); killErr != nil {
			log.Printf("runc kill failed for %s: %v", cfg.containerID, killErr)
		}
		err := <-waitCh
		logTiming("runc_wait", time.Since(startStart))
		delStart := time.Now()
		deleteContainer(cfg.runcBinary, cfg.containerID)
		logTiming("runc_delete", time.Since(delStart))
		logTiming("total", time.Since(totalStart))
		if err != nil {
			return fmt.Errorf("runc run interrupted by %s: %w", sig, err)
		}
		return fmt.Errorf("runc run interrupted by %s", sig)
	case <-time.After(cfg.timeout):
		timedOut = true
	}

	_ = exec.Command(cfg.runcBinary, "kill", cfg.containerID, "KILL").Run()
	err = <-waitCh
	logTiming("runc_wait", time.Since(startStart))
	delStart := time.Now()
	deleteContainer(cfg.runcBinary, cfg.containerID)
	logTiming("runc_delete", time.Since(delStart))
	logTiming("total", time.Since(totalStart))
	if err == nil {
		return fmt.Errorf("container timed out after %s", cfg.timeout)
	}
	if timedOut {
		return fmt.Errorf("container timed out after %s", cfg.timeout)
	}
	return err
}

func prepareBundle(rootfsAbs string, cfg config, cmdArgs []string) (string, func(), error) {
	bundleDir, err := os.MkdirTemp("", "afs-runc-bundle-*")
	if err != nil {
		return "", nil, fmt.Errorf("create bundle dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(bundleDir) }

	resolvedUser, err := resolveOCIUser(rootfsAbs, cfg.user)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("resolve user: %w", err)
	}
	spec := buildSpec(cfg, cmdArgs, rootfsAbs, resolvedUser)
	cfgPath := filepath.Join(bundleDir, "config.json")
	content, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("marshal config.json: %w", err)
	}
	content = append(content, '\n')
	if err := os.WriteFile(cfgPath, content, 0o644); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("write config.json: %w", err)
	}
	return bundleDir, cleanup, nil
}

func buildSpec(cfg config, cmdArgs []string, rootfsPath string, user ociUser) ociSpec {
	period := uint64(100000)
	quota := cfg.cpuCores * int64(period)
	memLimit := cfg.memoryMB * 1024 * 1024

	namespaces := []ociNS{{Type: "mount"}}
	if !cfg.noPIDNS {
		namespaces = append(namespaces, ociNS{Type: "pid"})
	}
	if !cfg.noIPCNS {
		namespaces = append(namespaces, ociNS{Type: "ipc"})
	}
	if !cfg.noUTSNS {
		namespaces = append(namespaces, ociNS{Type: "uts"})
	}
	if !cfg.noCgroupNS {
		namespaces = append(namespaces, ociNS{Type: "cgroup"})
	}
	hostname := ""
	if !cfg.noUTSNS {
		hostname = "afs-runc"
	}

	return ociSpec{
		Version: "1.0.2",
		Root: ociRoot{
			Path:     rootfsPath,
			Readonly: false,
		},
		Process: ociProcess{
			Terminal: false,
			Args:     cmdArgs,
			Cwd:      normalizeWorkingDir(cfg.cwd),
			Env:      normalizeProcessEnv(cfg.env),
			User:     user,
		},
		Hostname: hostname,
		Linux: ociLinux{
			CgroupsPath: filepath.Join("/afs_runc", cfg.containerID),
			Resources: ociResources{
				CPU:    ociCPU{Period: &period, Quota: &quota},
				Memory: ociMemory{Limit: &memLimit},
			},
			Namespaces: namespaces,
		},
		Mounts: defaultMounts(),
	}
}

func defaultMounts() []ociMount {
	return []ociMount{
		{Destination: "/proc", Type: "proc", Source: "proc"},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		{Destination: "/etc/resolv.conf", Type: "bind", Source: "/etc/resolv.conf", Options: []string{"rbind", "ro"}},
	}
}

func defaultContainerID() string {
	return fmt.Sprintf("afs-runc-%d-%06d", time.Now().UnixNano(), rand.Intn(1000000))
}

func normalizeProcessEnv(env []string) []string {
	out := dedupeEnvKeepLast(env)
	if !hasEnvKey(out, "PATH") {
		out = append(out, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return out
}

func dedupeEnvKeepLast(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	reversed := make([]string, 0, len(in))
	for i := len(in) - 1; i >= 0; i-- {
		v := strings.TrimSpace(in[i])
		if v == "" {
			continue
		}
		key := envKey(v)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		reversed = append(reversed, v)
	}
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	return reversed
}

func hasEnvKey(env []string, key string) bool {
	for _, entry := range env {
		if envKey(entry) == key {
			return true
		}
	}
	return false
}

func envKey(entry string) string {
	if idx := strings.IndexByte(entry, '='); idx >= 0 {
		return entry[:idx]
	}
	return entry
}

func normalizeWorkingDir(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "/"
	}
	if !strings.HasPrefix(v, "/") {
		return "/" + v
	}
	return v
}

func resolveOCIUser(rootfsPath string, userSpec string) (ociUser, error) {
	userSpec = strings.TrimSpace(userSpec)
	if userSpec == "" {
		return ociUser{UID: 0, GID: 0}, nil
	}

	userPart := userSpec
	groupPart := ""
	if idx := strings.IndexByte(userSpec, ':'); idx >= 0 {
		userPart = userSpec[:idx]
		groupPart = userSpec[idx+1:]
	}
	userPart = strings.TrimSpace(userPart)
	groupPart = strings.TrimSpace(groupPart)
	if userPart == "" {
		return ociUser{}, fmt.Errorf("user must not be empty")
	}

	passwdEntries, _ := loadPasswdEntries(rootfsPath)
	groupEntries, _ := loadGroupEntries(rootfsPath)

	var (
		user     ociUser
		username string
	)
	if uid, ok := parseUint32(userPart); ok {
		user.UID = uid
		if entry, found := findPasswdByUID(passwdEntries, uid); found {
			user.GID = entry.GID
			username = entry.Name
		}
	} else {
		entry, found := findPasswdByName(passwdEntries, userPart)
		if !found {
			return ociUser{}, fmt.Errorf("user %q not found in %s/etc/passwd", userPart, rootfsPath)
		}
		user.UID = entry.UID
		user.GID = entry.GID
		username = entry.Name
	}

	if groupPart != "" {
		gid, err := resolveGroupID(groupEntries, groupPart, rootfsPath)
		if err != nil {
			return ociUser{}, err
		}
		user.GID = gid
	} else if username != "" {
		user.AdditionalGIDs = supplementaryGIDs(groupEntries, username, user.GID)
	}
	return user, nil
}

func parseUint32(v string) (uint32, bool) {
	n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

func resolveGroupID(groups []groupEntry, groupSpec string, rootfsPath string) (uint32, error) {
	if gid, ok := parseUint32(groupSpec); ok {
		return gid, nil
	}
	for _, g := range groups {
		if g.Name == groupSpec {
			return g.GID, nil
		}
	}
	return 0, fmt.Errorf("group %q not found in %s/etc/group", groupSpec, rootfsPath)
}

func supplementaryGIDs(groups []groupEntry, username string, primaryGID uint32) []uint32 {
	seen := map[uint32]struct{}{primaryGID: {}}
	out := make([]uint32, 0)
	for _, g := range groups {
		if _, ok := seen[g.GID]; ok {
			continue
		}
		for _, member := range g.Members {
			if member == username {
				seen[g.GID] = struct{}{}
				out = append(out, g.GID)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func findPasswdByName(entries []passwdEntry, name string) (passwdEntry, bool) {
	for _, entry := range entries {
		if entry.Name == name {
			return entry, true
		}
	}
	return passwdEntry{}, false
}

func findPasswdByUID(entries []passwdEntry, uid uint32) (passwdEntry, bool) {
	for _, entry := range entries {
		if entry.UID == uid {
			return entry, true
		}
	}
	return passwdEntry{}, false
}

func loadPasswdEntries(rootfsPath string) ([]passwdEntry, error) {
	raw, err := os.ReadFile(filepath.Join(rootfsPath, "etc/passwd"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	lines := strings.Split(string(raw), "\n")
	out := make([]passwdEntry, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) < 4 {
			continue
		}
		uid, okUID := parseUint32(parts[2])
		gid, okGID := parseUint32(parts[3])
		if !okUID || !okGID {
			continue
		}
		out = append(out, passwdEntry{Name: parts[0], UID: uid, GID: gid})
	}
	return out, nil
}

func loadGroupEntries(rootfsPath string) ([]groupEntry, error) {
	raw, err := os.ReadFile(filepath.Join(rootfsPath, "etc/group"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	lines := strings.Split(string(raw), "\n")
	out := make([]groupEntry, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) < 3 {
			continue
		}
		gid, ok := parseUint32(parts[2])
		if !ok {
			continue
		}
		entry := groupEntry{Name: parts[0], GID: gid}
		if len(parts) >= 4 && strings.TrimSpace(parts[3]) != "" {
			for _, member := range strings.Split(parts[3], ",") {
				member = strings.TrimSpace(member)
				if member != "" {
					entry.Members = append(entry.Members, member)
				}
			}
		}
		out = append(out, entry)
	}
	return out, nil
}

func ensureDir(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return errors.New("not a directory")
	}
	return nil
}

func deleteContainer(runcBinary string, containerID string) {
	_ = exec.Command(runcBinary, "delete", "-f", containerID).Run()
}

type firstOutputDetector struct {
	start     time.Time
	emit      func(phase string, d time.Duration, stream string)
	onceByte  sync.Once
	onceLine  sync.Once
	lineMu    sync.Mutex
	lineBuf   []byte
	lineLimit int
}

func newFirstOutputDetector(start time.Time, emit func(phase string, d time.Duration, stream string)) *firstOutputDetector {
	return &firstOutputDetector{
		start:     start,
		emit:      emit,
		lineBuf:   make([]byte, 0, 256),
		lineLimit: 1 << 16,
	}
}

func (d *firstOutputDetector) Observe(stream string, p []byte) {
	if len(p) == 0 {
		return
	}
	d.onceByte.Do(func() {
		d.emit("first_output_byte", time.Since(d.start), stream)
	})
	d.lineMu.Lock()
	defer d.lineMu.Unlock()
	if len(d.lineBuf) > d.lineLimit {
		return
	}
	for _, b := range p {
		d.lineBuf = append(d.lineBuf, b)
		if b == '\n' {
			d.onceLine.Do(func() {
				d.emit("first_output_line", time.Since(d.start), stream)
			})
			return
		}
		if len(d.lineBuf) >= d.lineLimit {
			return
		}
	}
}

func copyObservedStream(stream string, r io.Reader, dst io.Writer, detector *firstOutputDetector, wg *sync.WaitGroup) {
	defer wg.Done()
	br := bufio.NewReaderSize(r, 32*1024)
	buf := make([]byte, 32*1024)
	for {
		n, err := br.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			detector.Observe(stream, chunk)
			if _, werr := dst.Write(chunk); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func logTiming(phase string, d time.Duration) {
	_, _ = fmt.Fprintf(os.Stderr, "__AFS_RUNC_TIMING__ phase=%s ms=%d\n", phase, d.Milliseconds())
}

func logTimingWithStream(phase string, d time.Duration, stream string) {
	stream = strings.TrimSpace(stream)
	if stream == "" {
		logTiming(phase, d)
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "__AFS_RUNC_TIMING__ phase=%s ms=%d stream=%s\n", phase, d.Milliseconds(), stream)
}
