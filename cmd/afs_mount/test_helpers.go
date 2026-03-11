package main

import (
	"fmt"
	"math/rand"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type serviceInfo struct {
	nodeID   string
	endpoint string
}

func rankServicesForAffinity(services []serviceInfo, nodeID string) []serviceInfo {
	local := make([]serviceInfo, 0, len(services))
	remote := make([]serviceInfo, 0, len(services))
	for _, s := range services {
		if nodeID != "" && s.nodeID == nodeID {
			local = append(local, s)
		} else {
			remote = append(remote, s)
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(remote), func(i, j int) { remote[i], remote[j] = remote[j], remote[i] })
	r.Shuffle(len(local), func(i, j int) { local[i], local[j] = local[j], local[i] })
	return append(local, remote...)
}

func effectiveLayerMountConcurrency(requested int, total int) int {
	if total <= 0 {
		return 1
	}
	if requested <= 0 {
		return 1
	}
	if requested > total {
		return total
	}
	return requested
}

func unmountCandidates(goos string, mountpoint string) [][]string {
	switch goos {
	case "linux":
		return [][]string{{"fusermount3", "-u", mountpoint}, {"fusermount", "-u", mountpoint}, {"umount", mountpoint}}
	case "darwin":
		return [][]string{{"umount", mountpoint}, {"diskutil", "unmount", "force", mountpoint}}
	default:
		return [][]string{{"umount", mountpoint}}
	}
}

type extraMountSpec struct {
	source string
	target string
}

func extraMountSpecs(goos string, mountpoint string, enabled bool) []extraMountSpec {
	if !enabled || goos != "linux" {
		return nil
	}
	return []extraMountSpec{
		{source: "/proc", target: filepath.Join(mountpoint, "proc")},
		{source: "/dev", target: filepath.Join(mountpoint, "dev")},
	}
}

func buildUnionMountCommand(goos string, layerDirs []string, mountpoint string, writableUpper string, writableWork string, extraDir string) (*exec.Cmd, error) {
	if len(layerDirs) == 0 {
		return nil, fmt.Errorf("no layer directories to compose")
	}
	extraDir = strings.TrimSpace(extraDir)
	ordered := reverseCopy(layerDirs)
	if goos == "linux" {
		if extraDir != "" {
			ordered = append([]string{extraDir}, ordered...)
		}
		lower := strings.Join(ordered, ":")
		opts := []string{"lowerdir=" + lower, "exec"}
		if strings.TrimSpace(writableUpper) != "" {
			if strings.TrimSpace(writableWork) == "" {
				return nil, fmt.Errorf("writable work dir is required for linux upperdir")
			}
			opts = append(opts, "upperdir="+writableUpper, "workdir="+writableWork)
		}
		return exec.Command("fuse-overlayfs", "-f", "-o", strings.Join(opts, ","), mountpoint), nil
	}
	if goos == "darwin" {
		branches := make([]string, 0, len(ordered)+1)
		if strings.TrimSpace(writableUpper) != "" {
			branches = append(branches, writableUpper+"=RW")
		}
		if extraDir != "" {
			branches = append(branches, extraDir+"=RO")
		}
		for _, dir := range ordered {
			branches = append(branches, dir+"=RO")
		}
		binary := "unionfs-fuse"
		if _, err := exec.LookPath(binary); err != nil {
			if _, altErr := exec.LookPath("unionfs"); altErr != nil {
				return nil, fmt.Errorf("unionfs-fuse not found (also tried unionfs)")
			}
			binary = "unionfs"
		}
		return exec.Command(binary, "-f", "-o", "cow,exec", strings.Join(branches, ":"), mountpoint), nil
	}
	return nil, fmt.Errorf("unsupported OS %s; only linux and darwin are supported", goos)
}

func reverseCopy(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
