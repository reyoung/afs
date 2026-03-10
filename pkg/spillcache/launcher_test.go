package spillcache

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestLauncherEnsureStartedWithExistingDaemon(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sock := filepath.Join(dir, "daemon.sock")
	go func() {
		_ = RunServer(ServerConfig{CacheDir: dir, SockPath: sock, MaxBytes: 1 << 20})
	}()
	client := NewClient(sock, time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := client.Ping(); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	launcher, err := NewLauncher(LauncherConfig{
		CacheDir:   dir,
		SockPath:   sock,
		MaxBytes:   1 << 20,
		BinaryPath: "not-used",
	})
	if err != nil {
		t.Fatalf("NewLauncher: %v", err)
	}

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl, ensureErr := launcher.EnsureStarted()
			if ensureErr != nil {
				errs <- ensureErr
				return
			}
			errs <- cl.Ping()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("EnsureStarted concurrent error: %v", err)
		}
	}
}

func TestLauncherEnsureStartedSpawnFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	launcher, err := NewLauncher(LauncherConfig{
		CacheDir:    dir,
		SockPath:    filepath.Join(dir, "daemon.sock"),
		MaxBytes:    1 << 20,
		BinaryPath:  "definitely-not-exist-123",
		StartupWait: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLauncher: %v", err)
	}
	if _, err := launcher.EnsureStarted(); err == nil {
		t.Fatalf("expected EnsureStarted error when daemon binary is missing")
	}
}

func TestLauncherEnsureStartedKeepsSocketWhenDaemonLikelyAlive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sock := filepath.Join(dir, "daemon.sock")
	ownerLock := filepath.Join(dir, "daemon.owner.lock")
	pidFile := filepath.Join(dir, "daemon.pid")
	if err := os.WriteFile(sock, []byte("placeholder"), 0o644); err != nil {
		t.Fatalf("write fake socket file: %v", err)
	}

	lockFD, err := os.OpenFile(ownerLock, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open owner lock: %v", err)
	}
	defer lockFD.Close()
	if err := syscall.Flock(int(lockFD.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("flock owner lock: %v", err)
	}
	defer syscall.Flock(int(lockFD.Fd()), syscall.LOCK_UN)

	startTime, err := readProcessStartTime(os.Getpid())
	if err != nil {
		startTime = ""
	}
	if err := writeDaemonPIDState(pidFile, daemonPIDState{PID: os.Getpid(), StartTime: startTime}); err != nil {
		t.Fatalf("write pid file: %v", err)
	}

	launcher, err := NewLauncher(LauncherConfig{
		CacheDir:          dir,
		SockPath:          sock,
		OwnerLockPath:     ownerLock,
		PIDFilePath:       pidFile,
		MaxBytes:          1 << 20,
		BinaryPath:        "definitely-not-used",
		StartupWait:       120 * time.Millisecond,
		ClientTimeout:     10 * time.Millisecond,
		PingAttempts:      1,
		PingRetryInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLauncher: %v", err)
	}

	if _, err := launcher.EnsureStarted(); err == nil {
		t.Fatalf("expected EnsureStarted error when daemon ping keeps failing")
	}
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("socket path should not be removed: %v", err)
	}
}
