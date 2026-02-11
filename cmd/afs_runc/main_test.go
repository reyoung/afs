package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestBuildSpecResources(t *testing.T) {
	t.Parallel()
	cfg := config{
		cpuCores:    2,
		memoryMB:    512,
		containerID: "cid-test",
	}
	s := buildSpec(cfg, []string{"/bin/echo", "hi"}, "/abs/rootfs")

	if s.Version != "1.0.2" {
		t.Fatalf("Version=%q, want 1.0.2", s.Version)
	}
	if s.Root.Path != "/abs/rootfs" {
		t.Fatalf("Root.Path=%q, want /abs/rootfs", s.Root.Path)
	}
	if len(s.Process.Args) != 2 || s.Process.Args[0] != "/bin/echo" {
		t.Fatalf("Process.Args=%v, want /bin/echo hi", s.Process.Args)
	}
	if s.Linux.CgroupsPath != filepath.Join("/afs_runc", "cid-test") {
		t.Fatalf("CgroupsPath=%q, want %q", s.Linux.CgroupsPath, filepath.Join("/afs_runc", "cid-test"))
	}
	if s.Linux.Resources.CPU.Quota == nil || *s.Linux.Resources.CPU.Quota != 200000 {
		t.Fatalf("CPU quota=%v, want 200000", s.Linux.Resources.CPU.Quota)
	}
	if s.Linux.Resources.Memory.Limit == nil || *s.Linux.Resources.Memory.Limit != 512*1024*1024 {
		t.Fatalf("Memory limit=%v, want %d", s.Linux.Resources.Memory.Limit, 512*1024*1024)
	}
}

func TestDefaultMountsHasProcAndDev(t *testing.T) {
	t.Parallel()
	mounts := defaultMounts()
	if len(mounts) == 0 {
		t.Fatalf("defaultMounts should not be empty")
	}
	foundProc := false
	foundDev := false
	foundResolv := false
	for _, m := range mounts {
		if m.Destination == "/proc" && m.Type == "proc" {
			foundProc = true
		}
		if m.Destination == "/dev" && m.Type == "tmpfs" {
			foundDev = true
		}
		if m.Destination == "/etc/resolv.conf" && m.Type == "bind" {
			foundResolv = true
		}
	}
	if !foundProc || !foundDev || !foundResolv {
		t.Fatalf("default mounts missing /proc or /dev or /etc/resolv.conf: %v", mounts)
	}
}

func TestDefaultContainerIDPrefix(t *testing.T) {
	t.Parallel()
	id := defaultContainerID()
	if len(id) < 9 || id[:9] != "afs-runc-" {
		t.Fatalf("id=%q, want prefix afs-runc-", id)
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	cfg := config{cpuCores: 1, memoryMB: 256, timeout: time.Second}
	if cfg.cpuCores != 1 || cfg.memoryMB != 256 || cfg.timeout != time.Second {
		t.Fatalf("unexpected defaults template: %+v", cfg)
	}
}

func TestBuildSpecUsesHostNetworkByDefault(t *testing.T) {
	t.Parallel()
	cfg := config{
		cpuCores:    1,
		memoryMB:    256,
		containerID: "cid-test",
	}
	s := buildSpec(cfg, []string{"/bin/true"}, "/abs/rootfs")
	for _, ns := range s.Linux.Namespaces {
		if ns.Type == "network" {
			t.Fatalf("unexpected network namespace in default spec: %+v", s.Linux.Namespaces)
		}
	}
}
