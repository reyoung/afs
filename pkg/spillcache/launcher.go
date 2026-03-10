package spillcache

import (
	"errors"
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
	CacheDir          string
	SockPath          string
	LockPath          string
	OwnerLockPath     string
	PIDFilePath       string
	MaxBytes          int64
	BinaryPath        string
	StartupWait       time.Duration
	ClientTimeout     time.Duration
	PingAttempts      int
	PingRetryInterval time.Duration
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
	if strings.TrimSpace(cfg.OwnerLockPath) == "" {
		cfg.OwnerLockPath = defaultOwnerLockPath(cfg.CacheDir)
	}
	if strings.TrimSpace(cfg.PIDFilePath) == "" {
		cfg.PIDFilePath = defaultPIDFilePath(cfg.CacheDir)
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
	if cfg.PingAttempts <= 0 {
		cfg.PingAttempts = 4
	}
	if cfg.PingRetryInterval <= 0 {
		cfg.PingRetryInterval = 100 * time.Millisecond
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
	if err := l.pingWithRetry(client); err == nil {
		return client, nil
	}

	alive, err := l.isDaemonLikelyAlive()
	if err != nil {
		return nil, err
	}
	if alive {
		deadline := time.Now().Add(l.cfg.StartupWait)
		for time.Now().Before(deadline) {
			if err := client.Ping(); err == nil {
				return client, nil
			}
			time.Sleep(l.cfg.PingRetryInterval)
		}
		return nil, fmt.Errorf("daemon appears alive but ping failed on %s", l.cfg.SockPath)
	}
	if err := os.Remove(l.cfg.SockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale daemon socket: %w", err)
	}
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

func (l *Launcher) pingWithRetry(client *Client) error {
	var lastErr error
	for i := 0; i < l.cfg.PingAttempts; i++ {
		if err := client.Ping(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if i+1 < l.cfg.PingAttempts {
			time.Sleep(l.cfg.PingRetryInterval)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("ping failed")
	}
	return lastErr
}

func (l *Launcher) isDaemonLikelyAlive() (bool, error) {
	lockFd, heldByOther, err := tryAcquireExclusiveFileLock(l.cfg.OwnerLockPath)
	if err != nil {
		return false, fmt.Errorf("check owner lock %s: %w", l.cfg.OwnerLockPath, err)
	}
	if heldByOther {
		return true, nil
	}
	releaseExclusiveFileLock(lockFd)

	st, err := readDaemonPIDState(l.cfg.PIDFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		// Corrupted/stale pid file should not block recovery.
		return false, nil
	}
	return isProcessAlive(st.PID, st.StartTime), nil
}

func (l *Launcher) spawnDaemon() error {
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", os.DevNull, err)
	}
	defer devNull.Close()

	cmd := exec.Command(l.cfg.BinaryPath,
		"-cache-dir", l.cfg.CacheDir,
		"-sock", l.cfg.SockPath,
		"-owner-lock", l.cfg.OwnerLockPath,
		"-pid-file", l.cfg.PIDFilePath,
		"-cache-max-bytes", strconv.FormatInt(l.cfg.MaxBytes, 10),
	)
	// Detach daemon stdio from the caller process, so it does not keep
	// inherited pipe FDs open (for example afslet -> afs_mount log pipes).
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start cache daemon %s: %w", l.cfg.BinaryPath, err)
	}
	return cmd.Process.Release()
}
