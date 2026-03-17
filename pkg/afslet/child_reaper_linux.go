//go:build linux

package afslet

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sys/unix"
)

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
				reapChildren(logger)
			case <-done:
				reapChildren(logger)
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
