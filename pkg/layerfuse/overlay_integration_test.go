//go:build linux

package layerfuse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
)

// skipIfNoFuse skips the test if FUSE is not available.
func skipIfNoFuse(t *testing.T) {
	t.Helper()
	if os.Getuid() != 0 {
		if _, err := exec.LookPath("fusermount3"); err != nil {
			t.Skip("fusermount3 not found, skipping FUSE test")
		}
	}
}

// testLayer describes a layer with files, dirs, and symlinks for test construction.
type testLayer struct {
	files    map[string]string // path → content (regular files)
	dirs     []string          // directory paths
	symlinks map[string]string // path → target
}

// createTestLayerReader builds a layerformat.Reader from a testLayer definition.
func createTestLayerReader(t *testing.T, layer testLayer) *layerformat.Reader {
	t.Helper()

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Write directories first.
	for _, d := range layer.dirs {
		if err := tw.WriteHeader(&tar.Header{
			Name:     d,
			Typeflag: tar.TypeDir,
			Mode:     0o755,
		}); err != nil {
			t.Fatalf("write dir header %s: %v", d, err)
		}
	}

	// Write regular files.
	// Sort keys for deterministic ordering.
	sortedFiles := make([]string, 0, len(layer.files))
	for k := range layer.files {
		sortedFiles = append(sortedFiles, k)
	}
	sort.Strings(sortedFiles)
	for _, name := range sortedFiles {
		data := []byte(layer.files[name])
		if err := tw.WriteHeader(&tar.Header{
			Name:     name,
			Typeflag: tar.TypeReg,
			Mode:     0o644,
			Size:     int64(len(data)),
		}); err != nil {
			t.Fatalf("write file header %s: %v", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatalf("write file data %s: %v", name, err)
		}
	}

	// Write symlinks.
	sortedSymlinks := make([]string, 0, len(layer.symlinks))
	for k := range layer.symlinks {
		sortedSymlinks = append(sortedSymlinks, k)
	}
	sort.Strings(sortedSymlinks)
	for _, name := range sortedSymlinks {
		if err := tw.WriteHeader(&tar.Header{
			Name:     name,
			Typeflag: tar.TypeSymlink,
			Linkname: layer.symlinks[name],
			Mode:     0o777,
		}); err != nil {
			t.Fatalf("write symlink header %s: %v", name, err)
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write gzip: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}

	var archive bytes.Buffer
	if err := layerformat.ConvertTarGzipToArchive(bytes.NewReader(gzBuf.Bytes()), &archive); err != nil {
		t.Fatalf("ConvertTarGzipToArchive: %v", err)
	}
	reader, err := layerformat.NewReader(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return reader
}

// mountOverlayRO mounts a read-only overlay from the given layers and returns the mount point.
func mountOverlayRO(t *testing.T, layers []testLayer) string {
	t.Helper()
	skipIfNoFuse(t)

	overlayLayers := make([]OverlayLayer, len(layers))
	for i, l := range layers {
		overlayLayers[i] = OverlayLayer{
			Reader: createTestLayerReader(t, l),
			Digest: fmt.Sprintf("test-layer-%d", i),
		}
	}

	root, _ := NewOverlayRoot(overlayLayers, nil)
	mountDir := t.TempDir()

	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      false,
		},
	})
	if err != nil {
		t.Fatalf("FUSE mount: %v", err)
	}
	t.Cleanup(func() {
		server.Unmount()
	})

	return mountDir
}

// mountOverlayRW mounts a read-write overlay from the given layers and extra files.
func mountOverlayRW(t *testing.T, layers []testLayer, extraFiles map[string]string) string {
	t.Helper()
	skipIfNoFuse(t)

	overlayLayers := make([]OverlayLayer, len(layers))
	for i, l := range layers {
		overlayLayers[i] = OverlayLayer{
			Reader: createTestLayerReader(t, l),
			Digest: fmt.Sprintf("test-layer-%d", i),
		}
	}

	upperDir := t.TempDir()
	extraDir := ""
	if len(extraFiles) > 0 {
		extraDir = t.TempDir()
		for path, content := range extraFiles {
			full := filepath.Join(extraDir, path)
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				t.Fatalf("MkdirAll for extra: %v", err)
			}
			if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
				t.Fatalf("WriteFile for extra: %v", err)
			}
		}
	}

	root, _ := NewOverlayRootRW(overlayLayers, nil, upperDir, extraDir)
	mountDir := t.TempDir()

	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      false,
		},
	})
	if err != nil {
		t.Fatalf("FUSE mount: %v", err)
	}
	t.Cleanup(func() {
		server.Unmount()
	})

	// Give the FUSE server a moment to become ready.
	server.WaitMount()

	return mountDir
}

// ──────────────────────────────────────────────────────────────────────────────
// Layer merge semantics (read-only)
// ──────────────────────────────────────────────────────────────────────────────

func TestOverlayMerge_UpperLayerOverridesLower(t *testing.T) {
	skipIfNoFuse(t)

	lower := testLayer{files: map[string]string{"hello.txt": "from-lower"}}
	upper := testLayer{files: map[string]string{"hello.txt": "from-upper"}}

	mnt := mountOverlayRO(t, []testLayer{lower, upper})

	data, err := os.ReadFile(filepath.Join(mnt, "hello.txt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "from-upper" {
		t.Fatalf("got %q, want %q", string(data), "from-upper")
	}
}

func TestOverlayMerge_WhiteoutDeletesFile(t *testing.T) {
	skipIfNoFuse(t)

	lower := testLayer{
		dirs:  []string{"dir"},
		files: map[string]string{"dir/secret.txt": "hidden"},
	}
	// Upper has a whiteout for dir/secret.txt
	upper := testLayer{
		files: map[string]string{"dir/.wh.secret.txt": ""},
	}

	mnt := mountOverlayRO(t, []testLayer{lower, upper})

	_, err := os.Stat(filepath.Join(mnt, "dir/secret.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT for whiteout file, got: %v", err)
	}

	// Whiteout file itself should not be visible.
	_, err = os.Stat(filepath.Join(mnt, "dir/.wh.secret.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("whiteout file should not be visible, got: %v", err)
	}
}

func TestOverlayMerge_OpaqueDir(t *testing.T) {
	skipIfNoFuse(t)

	lower := testLayer{
		dirs: []string{"dir"},
		files: map[string]string{
			"dir/a.txt": "aaa",
			"dir/b.txt": "bbb",
		},
	}
	// Upper layer has opaque marker plus a new file.
	upper := testLayer{
		files: map[string]string{
			"dir/.wh..wh..opq": "",
			"dir/c.txt":        "ccc",
		},
	}

	mnt := mountOverlayRO(t, []testLayer{lower, upper})

	// Lower files should be hidden.
	for _, name := range []string{"a.txt", "b.txt"} {
		_, err := os.Stat(filepath.Join(mnt, "dir", name))
		if !os.IsNotExist(err) {
			t.Fatalf("%s should be hidden by opaque dir, got err: %v", name, err)
		}
	}

	// Upper file should be visible.
	data, err := os.ReadFile(filepath.Join(mnt, "dir/c.txt"))
	if err != nil {
		t.Fatalf("ReadFile c.txt: %v", err)
	}
	if string(data) != "ccc" {
		t.Fatalf("got %q, want %q", string(data), "ccc")
	}

	// Opaque marker itself should not be visible.
	_, err = os.Stat(filepath.Join(mnt, "dir/.wh..wh..opq"))
	if !os.IsNotExist(err) {
		t.Fatalf("opaque marker should not be visible, got: %v", err)
	}
}

func TestOverlayMerge_MultipleLayersMerge(t *testing.T) {
	skipIfNoFuse(t)

	layer0 := testLayer{files: map[string]string{
		"base.txt": "layer0",
		"shared":   "v0",
	}}
	layer1 := testLayer{files: map[string]string{
		"mid.txt": "layer1",
		"shared":  "v1",
	}}
	layer2 := testLayer{files: map[string]string{
		"top.txt": "layer2",
		"shared":  "v2",
	}}

	mnt := mountOverlayRO(t, []testLayer{layer0, layer1, layer2})

	tests := map[string]string{
		"base.txt": "layer0",
		"mid.txt":  "layer1",
		"top.txt":  "layer2",
		"shared":   "v2", // topmost wins
	}
	for name, want := range tests {
		data, err := os.ReadFile(filepath.Join(mnt, name))
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", name, err)
		}
		if string(data) != want {
			t.Fatalf("ReadFile(%s) = %q, want %q", name, string(data), want)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Read operations (RW mode)
// ──────────────────────────────────────────────────────────────────────────────

func TestOverlayRW_ReadLayerFile(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{
		dirs:  []string{"src"},
		files: map[string]string{"src/main.c": "#include <stdio.h>\nint main() { return 0; }\n"},
	}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	data, err := os.ReadFile(filepath.Join(mnt, "src/main.c"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "stdio.h") {
		t.Fatalf("unexpected content: %q", string(data))
	}
}

func TestOverlayRW_ReadExtraFile(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{files: map[string]string{"base.txt": "base"}}
	extra := map[string]string{"extra.txt": "extra-content"}

	mnt := mountOverlayRW(t, []testLayer{layer}, extra)

	data, err := os.ReadFile(filepath.Join(mnt, "extra.txt"))
	if err != nil {
		t.Fatalf("ReadFile extra: %v", err)
	}
	if string(data) != "extra-content" {
		t.Fatalf("got %q, want %q", string(data), "extra-content")
	}
}

func TestOverlayRW_Readdir(t *testing.T) {
	skipIfNoFuse(t)

	// Test readdir merging: layer entries at root + extra file at root.
	// Note: when extra files create a subdirectory that shadows a layer
	// directory, the WritableDirNode takes over and only shows upper
	// entries. So we test at root level where OverlayDirNode handles merge.
	layer := testLayer{
		files: map[string]string{
			"layer1.txt": "l1",
			"layer2.txt": "l2",
		},
	}
	extra := map[string]string{"extra.txt": "ex"}

	mnt := mountOverlayRW(t, []testLayer{layer}, extra)

	entries, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	sort.Strings(names)

	want := []string{"extra.txt", "layer1.txt", "layer2.txt"}
	if len(names) != len(want) {
		t.Fatalf("Readdir got %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("Readdir[%d] = %q, want %q", i, names[i], want[i])
		}
	}
}

func TestOverlayRW_Readlink(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{
		dirs:     []string{"dir"},
		files:    map[string]string{"dir/real.txt": "real-content"},
		symlinks: map[string]string{"dir/link": "real.txt"},
	}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	target, err := os.Readlink(filepath.Join(mnt, "dir/link"))
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "real.txt" {
		t.Fatalf("Readlink = %q, want %q", target, "real.txt")
	}
}

func TestOverlayRW_Stat(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{
		dirs:  []string{"sub"},
		files: map[string]string{"sub/file.txt": "12345"},
	}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	// Stat regular file.
	fi, err := os.Stat(filepath.Join(mnt, "sub/file.txt"))
	if err != nil {
		t.Fatalf("Stat file: %v", err)
	}
	if fi.Size() != 5 {
		t.Fatalf("Size = %d, want 5", fi.Size())
	}
	if fi.IsDir() {
		t.Fatal("file should not be a directory")
	}

	// Stat directory.
	di, err := os.Stat(filepath.Join(mnt, "sub"))
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if !di.IsDir() {
		t.Fatal("sub should be a directory")
	}

	// Stat root.
	ri, err := os.Stat(mnt)
	if err != nil {
		t.Fatalf("Stat root: %v", err)
	}
	if !ri.IsDir() {
		t.Fatal("root should be a directory")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Write operations
// ──────────────────────────────────────────────────────────────────────────────

func TestOverlayRW_CreateFile(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{
		{files: map[string]string{"existing.txt": "old"}},
	}, nil)

	newPath := filepath.Join(mnt, "new.txt")
	content := "brand-new-content"
	if err := os.WriteFile(newPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	data, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != content {
		t.Fatalf("got %q, want %q", string(data), content)
	}
}

func TestOverlayRW_AppendFile(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	p := filepath.Join(mnt, "append.txt")
	// Create file with initial content.
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Append.
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile O_APPEND: %v", err)
	}
	if _, err := f.Write([]byte(" world")); err != nil {
		f.Close()
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("got %q, want %q", string(data), "hello world")
	}
}

func TestOverlayRW_Mkdir(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	dirPath := filepath.Join(mnt, "newdir")
	if err := os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	fi, err := os.Stat(dirPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !fi.IsDir() {
		t.Fatal("expected directory")
	}

	// Create a file inside the new directory.
	if err := os.WriteFile(filepath.Join(dirPath, "inner.txt"), []byte("inner"), 0o644); err != nil {
		t.Fatalf("WriteFile in newdir: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dirPath, "inner.txt"))
	if err != nil {
		t.Fatalf("ReadFile inner: %v", err)
	}
	if string(data) != "inner" {
		t.Fatalf("got %q, want %q", string(data), "inner")
	}
}

func TestOverlayRW_Unlink(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	p := filepath.Join(mnt, "todelete.txt")
	if err := os.WriteFile(p, []byte("bye"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	_, err := os.Stat(p)
	if !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT, got: %v", err)
	}
}

func TestOverlayRW_Rmdir(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	dirPath := filepath.Join(mnt, "rmme")
	if err := os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := syscall.Rmdir(dirPath); err != nil {
		t.Fatalf("Rmdir: %v", err)
	}
	_, err := os.Stat(dirPath)
	if !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT, got: %v", err)
	}
}

func TestOverlayRW_Rename(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	subdir := filepath.Join(mnt, "rdir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	oldPath := filepath.Join(subdir, "old.txt")
	// Note: we do NOT stat/lookup "new.txt" before the rename to avoid
	// the FUSE kernel caching a negative dentry for that name.
	newPath := filepath.Join(subdir, "new.txt")
	if err := os.WriteFile(oldPath, []byte("rename-me"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Rename should succeed.
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Old path should be gone.
	_, err := os.Stat(oldPath)
	if !os.IsNotExist(err) {
		t.Fatalf("old path should not exist, got: %v", err)
	}

	// Verify the renamed file is visible in readdir.
	entries, err := os.ReadDir(subdir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.Name() == "new.txt" {
			found = true
		}
		if e.Name() == "old.txt" {
			t.Fatal("old.txt should not appear in readdir")
		}
	}
	if !found {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("new.txt should appear in readdir, got %v", names)
	}
}

func TestOverlayRW_Symlink(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	target := filepath.Join(mnt, "target.txt")
	if err := os.WriteFile(target, []byte("target-data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	link := filepath.Join(mnt, "mylink")
	if err := os.Symlink("target.txt", link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	got, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if got != "target.txt" {
		t.Fatalf("Readlink = %q, want %q", got, "target.txt")
	}
}

func TestOverlayRW_Chmod(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	p := filepath.Join(mnt, "chmod.txt")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Chmod(p, 0o755); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Fatalf("Perm = %o, want 0755", fi.Mode().Perm())
	}
}

func TestOverlayRW_Truncate(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	p := filepath.Join(mnt, "trunc.txt")
	if err := os.WriteFile(p, []byte("long content here"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Truncate(p, 4); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "long" {
		t.Fatalf("got %q, want %q", string(data), "long")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Mixed operations
// ──────────────────────────────────────────────────────────────────────────────

func TestOverlayRW_WriteOverLayerFile(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{files: map[string]string{"config.txt": "old-config"}}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	p := filepath.Join(mnt, "config.txt")

	// Verify the layer content is readable first.
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile (before override): %v", err)
	}
	if string(data) != "old-config" {
		t.Fatalf("before override: got %q, want %q", string(data), "old-config")
	}

	// The layer file's OverlayFileNode rejects O_WRONLY. To override, we
	// first delete (which marks a whiteout) then create a new file.
	if err := os.Remove(p); err != nil {
		t.Fatalf("Remove layer file: %v", err)
	}
	if err := os.WriteFile(p, []byte("new-config"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	data, err = os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile (after override): %v", err)
	}
	if string(data) != "new-config" {
		t.Fatalf("after override: got %q, want %q", string(data), "new-config")
	}
}

func TestOverlayRW_DeleteLayerFile(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{files: map[string]string{"remove-me.txt": "goodbye"}}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	p := filepath.Join(mnt, "remove-me.txt")

	// Verify it exists.
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("file should exist before delete: %v", err)
	}

	// Delete.
	if err := os.Remove(p); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// Should be gone.
	_, err := os.Stat(p)
	if !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT after delete, got: %v", err)
	}
}

func TestOverlayRW_CreateAndReadInSubdir(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{
		dirs:  []string{"subdir"},
		files: map[string]string{"subdir/existing.txt": "exists"},
	}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	// Create a new file in the layer's subdirectory.
	newPath := filepath.Join(mnt, "subdir/new.txt")
	if err := os.WriteFile(newPath, []byte("new-in-subdir"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Read it back.
	data, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "new-in-subdir" {
		t.Fatalf("got %q, want %q", string(data), "new-in-subdir")
	}

	// Original file still readable.
	origData, err := os.ReadFile(filepath.Join(mnt, "subdir/existing.txt"))
	if err != nil {
		t.Fatalf("ReadFile existing: %v", err)
	}
	if string(origData) != "exists" {
		t.Fatalf("existing got %q, want %q", string(origData), "exists")
	}
}

func TestOverlayRW_CompileWorkflow(t *testing.T) {
	skipIfNoFuse(t)

	// Simulate a build workflow:
	// - Layer contains "headers" and "source" files
	// - Write operations create object files and a binary
	layer := testLayer{
		dirs: []string{"include", "src"},
		files: map[string]string{
			"include/stdio.h": "#pragma once\nvoid printf(const char *fmt, ...);\n",
			"include/math.h":  "#pragma once\ndouble sqrt(double x);\n",
			"src/main.cpp":    "#include <stdio.h>\n#include <math.h>\nint main() { return 0; }\n",
		},
	}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	// Step 1: "Read" header files (simulating preprocessor).
	headerData, err := os.ReadFile(filepath.Join(mnt, "include/stdio.h"))
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	if !strings.Contains(string(headerData), "printf") {
		t.Fatal("header should contain printf")
	}

	srcData, err := os.ReadFile(filepath.Join(mnt, "src/main.cpp"))
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	if !strings.Contains(string(srcData), "main") {
		t.Fatal("source should contain main")
	}

	// Step 2: "Compile" — create build directory and object file.
	buildDir := filepath.Join(mnt, "build")
	if err := os.Mkdir(buildDir, 0o755); err != nil {
		t.Fatalf("Mkdir build: %v", err)
	}
	objectContent := "FAKE_ELF_OBJECT_FILE_" + strings.Repeat("X", 1024)
	if err := os.WriteFile(filepath.Join(buildDir, "main.o"), []byte(objectContent), 0o644); err != nil {
		t.Fatalf("WriteFile main.o: %v", err)
	}

	// Step 3: "Link" — create binary.
	binaryContent := "FAKE_ELF_BINARY_" + strings.Repeat("Y", 2048)
	if err := os.WriteFile(filepath.Join(buildDir, "a.out"), []byte(binaryContent), 0o755); err != nil {
		t.Fatalf("WriteFile a.out: %v", err)
	}

	// Step 4: Verify all outputs.
	objData, err := os.ReadFile(filepath.Join(buildDir, "main.o"))
	if err != nil {
		t.Fatalf("ReadFile main.o: %v", err)
	}
	if string(objData) != objectContent {
		t.Fatal("object file content mismatch")
	}

	binData, err := os.ReadFile(filepath.Join(buildDir, "a.out"))
	if err != nil {
		t.Fatalf("ReadFile a.out: %v", err)
	}
	if string(binData) != binaryContent {
		t.Fatal("binary content mismatch")
	}

	fi, err := os.Stat(filepath.Join(buildDir, "a.out"))
	if err != nil {
		t.Fatalf("Stat a.out: %v", err)
	}
	if fi.Size() != int64(len(binaryContent)) {
		t.Fatalf("binary size = %d, want %d", fi.Size(), len(binaryContent))
	}

	// Step 5: Verify source files from layer are still intact.
	srcData2, err := os.ReadFile(filepath.Join(mnt, "src/main.cpp"))
	if err != nil {
		t.Fatalf("re-read source: %v", err)
	}
	if string(srcData2) != string(srcData) {
		t.Fatal("source file changed unexpectedly")
	}

	// Step 6: readdir build should show both files.
	buildEntries, err := os.ReadDir(buildDir)
	if err != nil {
		t.Fatalf("ReadDir build: %v", err)
	}
	buildNames := make([]string, len(buildEntries))
	for i, e := range buildEntries {
		buildNames[i] = e.Name()
	}
	sort.Strings(buildNames)
	wantBuild := []string{"a.out", "main.o"}
	if len(buildNames) != len(wantBuild) {
		t.Fatalf("build dir entries = %v, want %v", buildNames, wantBuild)
	}
	for i := range wantBuild {
		if buildNames[i] != wantBuild[i] {
			t.Fatalf("build[%d] = %q, want %q", i, buildNames[i], wantBuild[i])
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Edge cases
// ──────────────────────────────────────────────────────────────────────────────

func TestOverlayRW_LargeFileRoundTrip(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	// Write a 1MB file.
	size := 1 << 20
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	p := filepath.Join(mnt, "large.bin")
	if err := os.WriteFile(p, payload, 0o644); err != nil {
		t.Fatalf("WriteFile large: %v", err)
	}

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile large: %v", err)
	}
	if len(data) != size {
		t.Fatalf("size = %d, want %d", len(data), size)
	}
	for i := range data {
		if data[i] != payload[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, data[i], payload[i])
		}
	}
}

func TestOverlayRW_CreateDeleteRecreate(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	p := filepath.Join(mnt, "cycle.txt")

	// Create.
	if err := os.WriteFile(p, []byte("v1"), 0o644); err != nil {
		t.Fatalf("WriteFile v1: %v", err)
	}
	data, _ := os.ReadFile(p)
	if string(data) != "v1" {
		t.Fatalf("v1: got %q", string(data))
	}

	// Delete.
	if err := os.Remove(p); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT, got: %v", err)
	}

	// Re-create with different content.
	if err := os.WriteFile(p, []byte("v2"), 0o644); err != nil {
		t.Fatalf("WriteFile v2: %v", err)
	}
	data, _ = os.ReadFile(p)
	if string(data) != "v2" {
		t.Fatalf("v2: got %q", string(data))
	}
}

func TestOverlayRW_ReaddirAfterWriteAndDelete(t *testing.T) {
	skipIfNoFuse(t)

	layer := testLayer{
		dirs:  []string{"pkg"},
		files: map[string]string{"pkg/a.go": "package a\n"},
	}
	mnt := mountOverlayRW(t, []testLayer{layer}, nil)

	pkgDir := filepath.Join(mnt, "pkg")

	// Add a new file.
	if err := os.WriteFile(filepath.Join(pkgDir, "b.go"), []byte("package b\n"), 0o644); err != nil {
		t.Fatalf("WriteFile b.go: %v", err)
	}

	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	sort.Strings(names)
	if len(names) < 2 || names[0] != "a.go" || names[1] != "b.go" {
		t.Fatalf("after create: got %v, want [a.go b.go]", names)
	}

	// Delete a layer file.
	if err := os.Remove(filepath.Join(pkgDir, "a.go")); err != nil {
		t.Fatalf("Remove a.go: %v", err)
	}

	// Allow kernel cache to flush.
	time.Sleep(50 * time.Millisecond)

	entries2, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("ReadDir after delete: %v", err)
	}
	names2 := make([]string, len(entries2))
	for i, e := range entries2 {
		names2[i] = e.Name()
	}
	sort.Strings(names2)
	// a.go should be gone; b.go should remain.
	found := false
	for _, n := range names2 {
		if n == "a.go" {
			t.Fatal("a.go should have been deleted from readdir")
		}
		if n == "b.go" {
			found = true
		}
	}
	if !found {
		t.Fatalf("b.go should be in readdir, got %v", names2)
	}
}

func TestOverlayRW_WriteAndReadPiecewise(t *testing.T) {
	skipIfNoFuse(t)

	mnt := mountOverlayRW(t, []testLayer{}, nil)

	p := filepath.Join(mnt, "piecewise.txt")

	// Create and write in multiple Write calls through a file handle.
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	chunks := []string{"Hello, ", "World", "!", " Testing ", "piecewise writes."}
	for _, chunk := range chunks {
		if _, err := io.WriteString(f, chunk); err != nil {
			f.Close()
			t.Fatalf("WriteString: %v", err)
		}
	}
	f.Close()

	want := strings.Join(chunks, "")
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != want {
		t.Fatalf("got %q, want %q", string(data), want)
	}
}
