package afslet

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/afsletpb"
)

func TestRunCommandRequiresExplicitCPUAndMemory(t *testing.T) {
	t.Parallel()

	svc := NewService(Config{LimitCPUCores: 8, LimitMemoryMB: 2048})
	sess, cleanup, err := newSession(t.TempDir())
	if err != nil {
		t.Fatalf("newSession() error: %v", err)
	}
	defer cleanup()

	res := svc.runCommand(context.Background(), sess, &afsletpb.StartRequest{
		Image:    "alpine",
		Command:  []string{"echo", "ok"},
		CpuCores: 0,
		MemoryMb: 256,
	}, nil)
	if res.Success || !strings.Contains(res.Err, "cpu_cores must be > 0") {
		t.Fatalf("expected cpu validation error, got success=%v err=%q", res.Success, res.Err)
	}

	res = svc.runCommand(context.Background(), sess, &afsletpb.StartRequest{
		Image:    "alpine",
		Command:  []string{"echo", "ok"},
		CpuCores: 1,
		MemoryMb: 0,
	}, nil)
	if res.Success || !strings.Contains(res.Err, "memory_mb must be > 0") {
		t.Fatalf("expected memory validation error, got success=%v err=%q", res.Success, res.Err)
	}
}

func TestNormalizeImageAndTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		image     string
		tag       string
		wantImage string
		wantTag   string
		wantErr   bool
	}{
		{name: "image only", image: "alpine", tag: "", wantImage: "alpine", wantTag: "", wantErr: false},
		{name: "split image tag", image: "alpine:latest", tag: "", wantImage: "alpine", wantTag: "latest", wantErr: false},
		{name: "duplicate same tag", image: "alpine:latest", tag: "latest", wantImage: "alpine", wantTag: "latest", wantErr: false},
		{name: "registry port no tag", image: "127.0.0.1:5000/alpine", tag: "", wantImage: "127.0.0.1:5000/alpine", wantTag: "", wantErr: false},
		{name: "mismatch tag", image: "alpine:3.19", tag: "latest", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			img, tag, err := normalizeImageAndTag(tc.image, tc.tag)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if img != tc.wantImage || tag != tc.wantTag {
				t.Fatalf("got image=%q tag=%q, want image=%q tag=%q", img, tag, tc.wantImage, tc.wantTag)
			}
		})
	}
}

func TestPickDiscoveryAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  string
		def  string
		want string
	}{
		{name: "request overrides default", req: "10.0.0.1:60051", def: "127.0.0.1:60051", want: "10.0.0.1:60051"},
		{name: "fallback to default", req: "", def: "127.0.0.1:60051", want: "127.0.0.1:60051"},
		{name: "both empty", req: "", def: "", want: ""},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := pickDiscoveryAddr(tc.req, tc.def)
			if got != tc.want {
				t.Fatalf("pickDiscoveryAddr(%q,%q)=%q, want %q", tc.req, tc.def, got, tc.want)
			}
		})
	}
}

func TestNewSessionRespectsTempDir(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	sess, cleanup, err := newSession(base)
	if err != nil {
		t.Fatalf("newSession() error: %v", err)
	}
	defer cleanup()

	if !strings.HasPrefix(sess.root, filepath.Clean(base)+string(filepath.Separator)) {
		t.Fatalf("session root=%q should be under %q", sess.root, base)
	}
}

func TestGetRuntimeStatus(t *testing.T) {
	t.Parallel()

	svc := NewService(Config{LimitCPUCores: 8, LimitMemoryMB: 2048})
	release, err := svc.reserveResources(3, 512)
	if err != nil {
		t.Fatalf("reserveResources() error: %v", err)
	}
	defer release()
	svc.addRunningContainers(2)
	defer svc.addRunningContainers(-2)

	resp, err := svc.GetRuntimeStatus(context.Background(), &afsletpb.GetRuntimeStatusRequest{})
	if err != nil {
		t.Fatalf("GetRuntimeStatus() error: %v", err)
	}
	if resp.GetRunningContainers() != 2 {
		t.Fatalf("running_containers=%d, want 2", resp.GetRunningContainers())
	}
	if resp.GetLimitCpuCores() != 8 || resp.GetLimitMemoryMb() != 2048 {
		t.Fatalf("limits cpu=%d memory=%d, want cpu=8 memory=2048", resp.GetLimitCpuCores(), resp.GetLimitMemoryMb())
	}
	if resp.GetUsedCpuCores() != 3 || resp.GetUsedMemoryMb() != 512 {
		t.Fatalf("used cpu=%d memory=%d, want cpu=3 memory=512", resp.GetUsedCpuCores(), resp.GetUsedMemoryMb())
	}
	if resp.GetAvailableCpuCores() != 5 || resp.GetAvailableMemoryMb() != 1536 {
		t.Fatalf("available cpu=%d memory=%d, want cpu=5 memory=1536", resp.GetAvailableCpuCores(), resp.GetAvailableMemoryMb())
	}
}

func TestReserveResourcesRejectsOverLimit(t *testing.T) {
	t.Parallel()

	svc := NewService(Config{LimitCPUCores: 2, LimitMemoryMB: 256})
	if _, err := svc.reserveResources(3, 64); err == nil {
		t.Fatalf("expected cpu over-limit error, got nil")
	}
	if _, err := svc.reserveResources(1, 512); err == nil {
		t.Fatalf("expected memory over-limit error, got nil")
	}
}

func TestReserveResourcesRejectsWhenInsufficientRemaining(t *testing.T) {
	t.Parallel()

	svc := NewService(Config{LimitCPUCores: 4, LimitMemoryMB: 1024})
	release, err := svc.reserveResources(3, 900)
	if err != nil {
		t.Fatalf("first reserveResources() error: %v", err)
	}
	defer release()

	if _, err := svc.reserveResources(2, 64); err == nil {
		t.Fatalf("expected insufficient cpu error, got nil")
	}
	if _, err := svc.reserveResources(1, 200); err == nil {
		t.Fatalf("expected insufficient memory error, got nil")
	}
}

func TestWaitForMountReadyReturnsWhenMountProcessExits(t *testing.T) {
	t.Parallel()

	mountWait := make(chan error, 1)
	mountWait <- context.DeadlineExceeded

	err := waitForMountReady(t.TempDir(), mountWait, 5*time.Second, nil)
	if err == nil {
		t.Fatalf("expected error when mount process exits")
	}
	if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTerminateProcessDoesNotBlockAfterWaitConsumed(t *testing.T) {
	t.Parallel()

	cmd := exec.Command("sh", "-c", "exit 1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start command: %v", err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	// Simulate waitForMountReady having already consumed mountWait.
	select {
	case <-waitCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for command to exit")
	}

	start := time.Now()
	if err := terminateProcess(cmd, waitCh); err != nil {
		t.Fatalf("terminateProcess() returned unexpected error: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("terminateProcess blocked too long: %v", d)
	}
}
