package main

import (
	"os"
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
		user:        "1000:1001",
		cwd:         "/workspace",
		env:         multiStringFlag{"FOO=bar", "PATH=/custom/bin"},
	}
	s := buildSpec(cfg, []string{"/bin/echo", "hi"}, "/abs/rootfs", ociUser{UID: 1000, GID: 1001})

	if s.Version != "1.0.2" {
		t.Fatalf("Version=%q, want 1.0.2", s.Version)
	}
	if s.Root.Path != "/abs/rootfs" {
		t.Fatalf("Root.Path=%q, want /abs/rootfs", s.Root.Path)
	}
	if len(s.Process.Args) != 2 || s.Process.Args[0] != "/bin/echo" {
		t.Fatalf("Process.Args=%v, want /bin/echo hi", s.Process.Args)
	}
	if s.Process.Cwd != "/workspace" {
		t.Fatalf("Process.Cwd=%q, want /workspace", s.Process.Cwd)
	}
	if len(s.Process.Env) != 2 || s.Process.Env[0] != "FOO=bar" || s.Process.Env[1] != "PATH=/custom/bin" {
		t.Fatalf("Process.Env=%v, want injected env", s.Process.Env)
	}
	if s.Process.User.UID != 1000 || s.Process.User.GID != 1001 {
		t.Fatalf("Process.User=%+v, want uid=1000 gid=1001", s.Process.User)
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
	s := buildSpec(cfg, []string{"/bin/true"}, "/abs/rootfs", ociUser{})
	for _, ns := range s.Linux.Namespaces {
		if ns.Type == "network" {
			t.Fatalf("unexpected network namespace in default spec: %+v", s.Linux.Namespaces)
		}
	}
}

func TestResolveOCIUserDefaultsToRoot(t *testing.T) {
	t.Parallel()

	got, err := resolveOCIUser(t.TempDir(), "")
	if err != nil {
		t.Fatalf("resolveOCIUser() error: %v", err)
	}
	if got.UID != 0 || got.GID != 0 {
		t.Fatalf("got=%+v, want root", got)
	}
}

func TestResolveOCIUserByNameAndSupplementaryGroups(t *testing.T) {
	t.Parallel()

	rootfs := t.TempDir()
	etcDir := filepath.Join(rootfs, "etc")
	if err := os.MkdirAll(etcDir, 0o755); err != nil {
		t.Fatalf("mkdir etc: %v", err)
	}
	if err := os.WriteFile(filepath.Join(etcDir, "passwd"), []byte("memcache:x:11211:11211::/home/memcache:/sbin/nologin\n"), 0o644); err != nil {
		t.Fatalf("write passwd: %v", err)
	}
	if err := os.WriteFile(filepath.Join(etcDir, "group"), []byte("memcache:x:11211:\ncache:x:2000:memcache\n"), 0o644); err != nil {
		t.Fatalf("write group: %v", err)
	}

	got, err := resolveOCIUser(rootfs, "memcache")
	if err != nil {
		t.Fatalf("resolveOCIUser() error: %v", err)
	}
	if got.UID != 11211 || got.GID != 11211 {
		t.Fatalf("got=%+v, want uid/gid 11211", got)
	}
	if len(got.AdditionalGIDs) != 1 || got.AdditionalGIDs[0] != 2000 {
		t.Fatalf("additional_gids=%v, want [2000]", got.AdditionalGIDs)
	}
}

func TestResolveOCIUserByNumericUIDAndExplicitGroup(t *testing.T) {
	t.Parallel()

	rootfs := t.TempDir()
	etcDir := filepath.Join(rootfs, "etc")
	if err := os.MkdirAll(etcDir, 0o755); err != nil {
		t.Fatalf("mkdir etc: %v", err)
	}
	if err := os.WriteFile(filepath.Join(etcDir, "group"), []byte("cache:x:2000:\n"), 0o644); err != nil {
		t.Fatalf("write group: %v", err)
	}

	got, err := resolveOCIUser(rootfs, "1234:cache")
	if err != nil {
		t.Fatalf("resolveOCIUser() error: %v", err)
	}
	if got.UID != 1234 || got.GID != 2000 {
		t.Fatalf("got=%+v, want uid=1234 gid=2000", got)
	}
	if len(got.AdditionalGIDs) != 0 {
		t.Fatalf("additional_gids=%v, want none", got.AdditionalGIDs)
	}
}

func TestNormalizeProcessEnvAddsDefaultPath(t *testing.T) {
	t.Parallel()

	got := normalizeProcessEnv([]string{"FOO=bar"})
	if len(got) != 2 || got[0] != "FOO=bar" || got[1] != "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" {
		t.Fatalf("normalizeProcessEnv=%v", got)
	}
}

func TestNormalizeWorkingDirDefaultsToRoot(t *testing.T) {
	t.Parallel()

	if got := normalizeWorkingDir(""); got != "/" {
		t.Fatalf("normalizeWorkingDir(\"\")=%q, want /", got)
	}
	if got := normalizeWorkingDir("work"); got != "/work" {
		t.Fatalf("normalizeWorkingDir(\"work\")=%q, want /work", got)
	}
}
