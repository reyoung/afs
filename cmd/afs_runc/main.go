package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type config struct {
	rootfs      string
	cpuCores    int64
	memoryMB    int64
	timeout     time.Duration
	runcBinary  string
	containerID string
	keepBundle  bool
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
	flag.BoolVar(&cfg.keepBundle, "keep-bundle", false, "keep generated OCI bundle for debugging")
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

	bundleDir, cleanup, err := prepareBundle(rootfsAbs, cfg, cmdArgs)
	if err != nil {
		return err
	}
	if !cfg.keepBundle {
		defer cleanup()
	}

	runCmd := exec.Command(cfg.runcBinary, "run", "--bundle", bundleDir, cfg.containerID)
	runCmd.Stdout = os.Stdout
	runCmd.Stderr = os.Stderr
	runCmd.Stdin = os.Stdin
	if err := runCmd.Start(); err != nil {
		return fmt.Errorf("start runc run: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- runCmd.Wait() }()
	timedOut := false

	select {
	case err := <-waitCh:
		deleteContainer(cfg.runcBinary, cfg.containerID)
		if err != nil {
			return fmt.Errorf("runc run failed: %w", err)
		}
		return nil
	case <-time.After(cfg.timeout):
		timedOut = true
	}

	_ = exec.Command(cfg.runcBinary, "kill", cfg.containerID, "KILL").Run()
	err = <-waitCh
	deleteContainer(cfg.runcBinary, cfg.containerID)
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

	spec := buildSpec(cfg, cmdArgs, rootfsAbs)
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

func buildSpec(cfg config, cmdArgs []string, rootfsPath string) ociSpec {
	period := uint64(100000)
	quota := cfg.cpuCores * int64(period)
	memLimit := cfg.memoryMB * 1024 * 1024

	return ociSpec{
		Version: "1.0.2",
		Root: ociRoot{
			Path:     rootfsPath,
			Readonly: false,
		},
		Process: ociProcess{
			Terminal: false,
			Args:     cmdArgs,
			Cwd:      "/",
			Env: []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			},
		},
		Hostname: "afs-runc",
		Linux: ociLinux{
			CgroupsPath: filepath.Join("/afs_runc", cfg.containerID),
			Resources: ociResources{
				CPU:    ociCPU{Period: &period, Quota: &quota},
				Memory: ociMemory{Limit: &memLimit},
			},
			Namespaces: []ociNS{
				{Type: "pid"},
				{Type: "network"},
				{Type: "ipc"},
				{Type: "uts"},
				{Type: "mount"},
				{Type: "cgroup"},
			},
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
	}
}

func defaultContainerID() string {
	return fmt.Sprintf("afs-runc-%d-%06d", time.Now().UnixNano(), rand.Intn(1000000))
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
