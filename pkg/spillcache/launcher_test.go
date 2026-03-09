package spillcache

import (
	"path/filepath"
	"sync"
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
