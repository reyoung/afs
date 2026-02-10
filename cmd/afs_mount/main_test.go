package main

import (
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestReverseCopy(t *testing.T) {
	t.Parallel()
	in := []string{"a", "b", "c"}
	got := reverseCopy(in)
	want := []string{"c", "b", "a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reverseCopy()=%v, want %v", got, want)
	}
	if !reflect.DeepEqual(in, []string{"a", "b", "c"}) {
		t.Fatalf("reverseCopy modified input: %v", in)
	}
}

func TestBuildUnionMountCommandLinux(t *testing.T) {
	t.Parallel()
	cmd, err := buildUnionMountCommand("linux", []string{"/l1", "/l2", "/l3"}, "/mnt", "/upper", "/work")
	if err != nil {
		t.Fatalf("buildUnionMountCommand() error: %v", err)
	}
	if filepath.Base(cmd.Path) != "fuse-overlayfs" {
		t.Fatalf("Path=%q, want base fuse-overlayfs", cmd.Path)
	}
	wantArgs := []string{"fuse-overlayfs", "-f", "-o", "lowerdir=/l3:/l2:/l1,exec,upperdir=/upper,workdir=/work", "/mnt"}
	if !reflect.DeepEqual(cmd.Args, wantArgs) {
		t.Fatalf("Args=%v, want %v", cmd.Args, wantArgs)
	}
}

func TestBuildUnionMountCommandUnsupportedOS(t *testing.T) {
	t.Parallel()
	if _, err := buildUnionMountCommand("windows", []string{"/l1"}, "/mnt", "", ""); err == nil {
		t.Fatalf("expected error for unsupported OS")
	}
}

func TestBuildUnionMountCommandLinuxMissingWorkdir(t *testing.T) {
	t.Parallel()
	if _, err := buildUnionMountCommand("linux", []string{"/l1"}, "/mnt", "/upper", ""); err == nil {
		t.Fatalf("expected error when linux upperdir has no workdir")
	}
}

func TestBuildUnionMountCommandDarwinBranches(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("unionfs-fuse"); err != nil {
		if _, altErr := exec.LookPath("unionfs"); altErr != nil {
			t.Skip("unionfs-fuse/unionfs not installed")
		}
	}

	cmd, err := buildUnionMountCommand("darwin", []string{"/l1", "/l2"}, "/mnt", "/upper", "")
	if err != nil {
		t.Fatalf("buildUnionMountCommand() error: %v", err)
	}
	args := strings.Join(cmd.Args, " ")
	if !strings.Contains(args, "-o cow,exec") {
		t.Fatalf("darwin args should include cow,exec: %v", cmd.Args)
	}
	if !strings.Contains(args, "/upper=RW:/l2=RO:/l1=RO") {
		t.Fatalf("darwin branch order mismatch: %v", cmd.Args)
	}
}

func TestUnmountCandidatesLinux(t *testing.T) {
	t.Parallel()
	got := unmountCandidates("linux", "/mnt")
	want := [][]string{
		{"fusermount3", "-u", "/mnt"},
		{"fusermount", "-u", "/mnt"},
		{"umount", "/mnt"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unmountCandidates(linux)=%v, want %v", got, want)
	}
}

func TestUnmountCandidatesDarwin(t *testing.T) {
	t.Parallel()
	got := unmountCandidates("darwin", "/mnt")
	want := [][]string{
		{"umount", "/mnt"},
		{"diskutil", "unmount", "force", "/mnt"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unmountCandidates(darwin)=%v, want %v", got, want)
	}
}

func TestRankServicesForAffinity(t *testing.T) {
	t.Parallel()

	ordered := rankServicesForAffinity([]serviceInfo{
		{nodeID: "remote-1", endpoint: "10.0.0.2:50051"},
		{nodeID: "local", endpoint: "10.0.0.1:50051"},
		{nodeID: "remote-2", endpoint: "10.0.0.3:50051"},
	}, "local")

	if len(ordered) != 3 {
		t.Fatalf("ordered length=%d, want 3", len(ordered))
	}
	if ordered[0].nodeID != "local" {
		t.Fatalf("expected local node first, got %s", ordered[0].nodeID)
	}
}

func TestExtraMountSpecsLinuxEnabled(t *testing.T) {
	t.Parallel()
	got := extraMountSpecs("linux", "/mnt/rootfs", true)
	want := []extraMountSpec{
		{source: "/proc", target: "/mnt/rootfs/proc"},
		{source: "/dev", target: "/mnt/rootfs/dev"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extraMountSpecs(linux,true)=%v, want %v", got, want)
	}
}

func TestExtraMountSpecsDisabledOrNonLinux(t *testing.T) {
	t.Parallel()
	if got := extraMountSpecs("linux", "/mnt/rootfs", false); got != nil {
		t.Fatalf("extraMountSpecs disabled should be nil, got %v", got)
	}
	if got := extraMountSpecs("darwin", "/mnt/rootfs", true); got != nil {
		t.Fatalf("extraMountSpecs non-linux should be nil, got %v", got)
	}
}
