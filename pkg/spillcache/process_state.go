package spillcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

const (
	daemonOwnerLockFileName = "daemon.owner.lock"
	daemonPIDFileName       = "daemon.pid"
)

type daemonPIDState struct {
	PID       int    `json:"pid"`
	StartTime string `json:"start_time,omitempty"`
}

func defaultOwnerLockPath(cacheDir string) string {
	return filepath.Join(cacheDir, daemonOwnerLockFileName)
}

func defaultPIDFilePath(cacheDir string) string {
	return filepath.Join(cacheDir, daemonPIDFileName)
}

func tryAcquireExclusiveFileLock(path string) (*os.File, bool, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = fd.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, true, nil
		}
		return nil, false, err
	}
	return fd, false, nil
}

func releaseExclusiveFileLock(fd *os.File) {
	if fd == nil {
		return
	}
	_ = syscall.Flock(int(fd.Fd()), syscall.LOCK_UN)
	_ = fd.Close()
}

func writeDaemonPIDState(path string, st daemonPIDState) error {
	if st.PID <= 0 {
		return fmt.Errorf("invalid daemon pid: %d", st.PID)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create daemon pid dir: %w", err)
	}
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal daemon pid state: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write daemon pid temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("commit daemon pid file: %w", err)
	}
	return nil
}

func readDaemonPIDState(path string) (daemonPIDState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return daemonPIDState{}, err
	}
	var st daemonPIDState
	if err := json.Unmarshal(data, &st); err != nil {
		return daemonPIDState{}, fmt.Errorf("parse daemon pid state: %w", err)
	}
	if st.PID <= 0 {
		return daemonPIDState{}, fmt.Errorf("invalid daemon pid in pid file")
	}
	return st, nil
}

func isProcessAlive(pid int, expectedStartTime string) bool {
	if pid <= 0 {
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false
		}
		if !errors.Is(err, syscall.EPERM) {
			return false
		}
	}
	expectedStartTime = strings.TrimSpace(expectedStartTime)
	if expectedStartTime == "" {
		return true
	}
	actualStartTime, err := readProcessStartTime(pid)
	if err != nil {
		return false
	}
	return actualStartTime == expectedStartTime
}

func readProcessStartTime(pid int) (string, error) {
	if runtime.GOOS != "linux" {
		return "", nil
	}
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", err
	}
	line := string(data)
	closeIdx := strings.LastIndex(line, ")")
	if closeIdx < 0 || closeIdx+1 >= len(line) {
		return "", fmt.Errorf("unexpected /proc stat format")
	}
	rest := strings.TrimSpace(line[closeIdx+1:])
	fields := strings.Fields(rest)
	if len(fields) <= 19 {
		return "", fmt.Errorf("unexpected /proc stat field count")
	}
	return fields[19], nil
}
