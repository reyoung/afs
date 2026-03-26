//go:build linux

package afslet

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// reapMu protects against the child reaper racing with exec.Cmd.Wait()
// (e.g. fusermount3 started by go-fuse). FUSE mount holds RLock during
// subprocess creation+wait; the reaper holds full Lock while calling Wait4(-1).
var reapMu sync.RWMutex

// HoldReaper returns a function that, when called, releases the reaper hold.
// While held, the child reaper will not call Wait4, preventing it from
// stealing child processes that other goroutines are waiting on.
func HoldReaper() func() {
	reapMu.RLock()
	return reapMu.RUnlock
}

func StartChildReaper(logger func(string, ...any)) func() {
	if logger == nil {
		logger = log.Printf
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		logger("enable child subreaper failed: %v", err)
		return func() {}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGCHLD)
	done := make(chan struct{})

	go func() {
		defer signal.Stop(sigCh)
		for {
			select {
			case <-sigCh:
				reapMu.Lock()
				reapChildren(logger)
				reapMu.Unlock()
			case <-done:
				reapMu.Lock()
				reapChildren(logger)
				reapMu.Unlock()
				return
			}
		}
	}()

	return func() {
		close(done)
	}
}

func reapChildren(logger func(string, ...any)) {
	for {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
		if err == nil {
			if pid <= 0 {
				return
			}
			continue
		}
		if err == syscall.ECHILD {
			return
		}
		logger("reap child failed: %v", err)
		return
	}
}
