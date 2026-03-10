package afsmount

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWaitForUnionMountReadyReadyProbeSuccess(t *testing.T) {
	t.Parallel()

	waitCh := make(chan error, 1)
	probeCalls := 0
	err := waitForUnionMountReady(context.Background(), "/tmp/mountpoint", waitCh, 200*time.Millisecond, func(_ string) (bool, error) {
		probeCalls++
		return probeCalls >= 3, nil
	})
	if err != nil {
		t.Fatalf("waitForUnionMountReady() error: %v", err)
	}
	if probeCalls < 3 {
		t.Fatalf("probeCalls=%d, want >=3", probeCalls)
	}
}

func TestWaitForUnionMountReadyMountProcessExit(t *testing.T) {
	t.Parallel()

	waitCh := make(chan error, 1)
	waitCh <- errors.New("mount exited")

	err := waitForUnionMountReady(context.Background(), "/tmp/mountpoint", waitCh, 200*time.Millisecond, func(_ string) (bool, error) {
		return false, nil
	})
	if err == nil {
		t.Fatalf("expected mount exit error")
	}
	if !strings.Contains(err.Error(), "mount exited") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForUnionMountReadyContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	waitCh := make(chan error, 1)
	err := waitForUnionMountReady(ctx, "/tmp/mountpoint", waitCh, 200*time.Millisecond, func(_ string) (bool, error) {
		return false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err=%v, want context.Canceled", err)
	}
}

func TestWaitForUnionMountReadyTimeoutIncludesLastProbeError(t *testing.T) {
	t.Parallel()

	waitCh := make(chan error, 1)
	err := waitForUnionMountReady(context.Background(), "/tmp/mountpoint", waitCh, 50*time.Millisecond, func(_ string) (bool, error) {
		return false, errors.New("probe failed")
	})
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timeout waiting for union mount ready") {
		t.Fatalf("unexpected timeout error: %v", err)
	}
	if !strings.Contains(err.Error(), "probe failed") {
		t.Fatalf("timeout error should include probe failure detail: %v", err)
	}
}
