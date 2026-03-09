package spillcache

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type LauncherConfig struct {
	CacheDir      string
	SockPath      string
	LockPath      string
	MaxBytes      int64
	BinaryPath    string
	StartupWait   time.Duration
	ClientTimeout time.Duration
}

type Launcher struct {
	cfg LauncherConfig
}

func NewLauncher(cfg LauncherConfig) (*Launcher, error) {
	if strings.TrimSpace(cfg.CacheDir) == "" {
		return nil, fmt.Errorf("cache dir is required")
	}
	if strings.TrimSpace(cfg.SockPath) == "" {
		cfg.SockPath = filepath.Join(cfg.CacheDir, "daemon.sock")
	}
	if strings.TrimSpace(cfg.LockPath) == "" {
		cfg.LockPath = filepath.Join(cfg.CacheDir, "daemon.lock")
	}
	if cfg.MaxBytes <= 0 {
		return nil, fmt.Errorf("max bytes must be > 0")
	}
	if strings.TrimSpace(cfg.BinaryPath) == "" {
		cfg.BinaryPath = "afs_mount_cached"
	}
	if cfg.StartupWait <= 0 {
		cfg.StartupWait = 5 * time.Second
	}
	if cfg.ClientTimeout <= 0 {
		cfg.ClientTimeout = 2 * time.Second
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	return &Launcher{cfg: cfg}, nil
}

func (l *Launcher) EnsureStarted() (*Client, error) {
	fd, err := os.OpenFile(l.cfg.LockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	defer fd.Close()
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX); err != nil {
		return nil, fmt.Errorf("flock lock: %w", err)
	}
	defer syscall.Flock(int(fd.Fd()), syscall.LOCK_UN)

	client := NewClient(l.cfg.SockPath, l.cfg.ClientTimeout)
	if err := client.Ping(); err == nil {
		return client, nil
	}
	_ = os.Remove(l.cfg.SockPath)
	if err := l.spawnDaemon(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(l.cfg.StartupWait)
	for time.Now().Before(deadline) {
		if err := client.Ping(); err == nil {
			return client, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("wait daemon startup timeout: %s", l.cfg.SockPath)
}

func (l *Launcher) spawnDaemon() error {
	cmd := exec.Command(l.cfg.BinaryPath,
		"-cache-dir", l.cfg.CacheDir,
		"-sock", l.cfg.SockPath,
		"-cache-max-bytes", strconv.FormatInt(l.cfg.MaxBytes, 10),
	)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start cache daemon %s: %w", l.cfg.BinaryPath, err)
	}
	return cmd.Process.Release()
}
