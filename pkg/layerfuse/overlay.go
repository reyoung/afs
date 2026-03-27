package layerfuse

import (
	"context"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/pagecache"
)

// OverlayLayer describes a single layer for the unified overlay.
type OverlayLayer struct {
	Reader *layerformat.Reader
	Digest string
}

// overlayEntry is a merged entry that knows which layer it came from.
type overlayEntry struct {
	entry      layerformat.Entry
	layerIndex int
	reader     *layerformat.Reader
}

// overlayTree holds the merged state of all layers.
type overlayTree struct {
	store    *pagecache.Store
	stats    *FuseStats
	entries  map[string]overlayEntry
	children map[string][]string
	kinds    map[string]uint32

	// RW support fields (only used when upperDir != "").
	upperDir string              // writable upper directory (empty = read-only mode)
	deleted  map[string]struct{} // paths marked as deleted (in-memory whiteout)
	mu       sync.RWMutex        // protects deleted map and upper dir operations
}

// NewOverlayRoot creates a single FUSE tree from multiple layers (bottom-to-top order).
// It handles:
// - Upper layer entries override lower layer entries at same path
// - ".wh.NAME" whiteout files delete NAME from lower layers
// - ".wh..wh..opq" opaque markers hide all lower layer entries in that directory
// - Whiteout files themselves are not exposed
func NewOverlayRoot(layers []OverlayLayer, store *pagecache.Store) (*OverlayDirNode, *FuseStats) {
	stats := &FuseStats{}
	t := buildOverlayTree(layers, store, stats)
	return &OverlayDirNode{tree: t, relPath: ""}, stats
}

// NewOverlayRootRW creates a unified FUSE tree with read-write support.
// upperDir is the writable upper directory on local disk.
// extraDir, if non-empty, is scanned and its contents are added to the merged tree.
func NewOverlayRootRW(layers []OverlayLayer, store *pagecache.Store, upperDir string, extraDir string) (*OverlayDirNode, *FuseStats) {
	stats := &FuseStats{}
	t := buildOverlayTree(layers, store, stats)
	t.upperDir = upperDir
	t.deleted = make(map[string]struct{})

	// If extraDir is non-empty, scan its contents and copy into upper dir
	// so they appear in the merged view and are writable.
	if extraDir != "" {
		copyExtraDirToUpper(extraDir, upperDir)
	}

	return &OverlayDirNode{tree: t, relPath: ""}, stats
}

// copyExtraDirToUpper recursively copies files from extraDir into the upper dir.
func copyExtraDirToUpper(extraDir, upperDir string) {
	log.Printf("copyExtraDirToUpper: extraDir=%s upperDir=%s", extraDir, upperDir)
	_ = filepath.Walk(extraDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		rel, relErr := filepath.Rel(extraDir, p)
		if relErr != nil || rel == "." {
			return nil
		}
		target := filepath.Join(upperDir, rel)
		if info.IsDir() {
			_ = os.MkdirAll(target, info.Mode())
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			link, linkErr := os.Readlink(p)
			if linkErr == nil {
				_ = os.MkdirAll(filepath.Dir(target), 0o755)
				_ = os.Symlink(link, target)
			}
			return nil
		}
		// Regular file: copy.
		_ = os.MkdirAll(filepath.Dir(target), 0o755)
		src, openErr := os.Open(p)
		if openErr != nil {
			log.Printf("copyExtraDirToUpper: failed to open %s: %v", p, openErr)
			return nil
		}
		defer src.Close()
		dst, createErr := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if createErr != nil {
			log.Printf("copyExtraDirToUpper: failed to create %s: %v", target, createErr)
			return nil
		}
		defer dst.Close()
		_, _ = io.Copy(dst, src)
		return nil
	})
}

func buildOverlayTree(layers []OverlayLayer, store *pagecache.Store, stats *FuseStats) *overlayTree {
	t := &overlayTree{
		store:    store,
		stats:    stats,
		entries:  make(map[string]overlayEntry),
		children: make(map[string][]string),
		kinds:    make(map[string]uint32),
	}

	// Track opaque directories per layer. When we see .wh..wh..opq in a dir,
	// all entries from lower layers in that dir are hidden.
	// Process layers bottom-to-top (index 0 = bottom).
	for layerIdx, layer := range layers {
		for _, e := range layer.Reader.Entries() {
			dir := path.Dir(e.Path)
			if dir == "." {
				dir = ""
			}
			name := path.Base(e.Path)

			// Handle opaque whiteout: marks the directory as opaque,
			// hiding all lower layer entries in that directory.
			if name == ".wh..wh..opq" {
				// Remove all entries from lower layers that are children of this dir.
				t.clearChildrenOf(dir)
				continue
			}

			// Handle individual whiteout: ".wh.NAME" deletes NAME from merged view.
			if strings.HasPrefix(name, ".wh.") {
				targetName := strings.TrimPrefix(name, ".wh.")
				targetPath := targetName
				if dir != "" {
					targetPath = dir + "/" + targetName
				}
				t.removeEntry(targetPath)
				continue
			}

			// Regular entry: add or override.
			t.entries[e.Path] = overlayEntry{
				entry:      e,
				layerIndex: layerIdx,
				reader:     layer.Reader,
			}
		}
	}

	// Now rebuild children and kinds maps from the merged entries.
	seenChild := make(map[string]map[string]struct{})

	ensureDir := func(p string) {}
	ensureDir = func(p string) {
		if p == "." {
			p = ""
		}
		if p == "" {
			return
		}
		if _, ok := t.entries[p]; !ok {
			t.entries[p] = overlayEntry{
				entry: layerformat.Entry{Path: p, Type: layerformat.EntryTypeDir, Mode: 0o755},
			}
			t.kinds[p] = syscall.S_IFDIR
		}
		parent := path.Dir(p)
		if parent == "." {
			parent = ""
		}
		name := path.Base(p)
		if seenChild[parent] == nil {
			seenChild[parent] = make(map[string]struct{})
		}
		if _, ok := seenChild[parent][name]; !ok {
			seenChild[parent][name] = struct{}{}
			t.children[parent] = append(t.children[parent], name)
		}
		if parent != "" {
			ensureDir(parent)
		}
	}

	for _, oe := range t.entries {
		e := oe.entry
		parent := path.Dir(e.Path)
		if parent == "." {
			parent = ""
		}
		ensureDir(parent)
		if seenChild[parent] == nil {
			seenChild[parent] = make(map[string]struct{})
		}
		name := path.Base(e.Path)
		if _, ok := seenChild[parent][name]; !ok {
			seenChild[parent][name] = struct{}{}
			t.children[parent] = append(t.children[parent], name)
		}

		switch e.Type {
		case layerformat.EntryTypeDir:
			t.kinds[e.Path] = syscall.S_IFDIR
		case layerformat.EntryTypeSymlink:
			t.kinds[e.Path] = syscall.S_IFLNK
		default:
			t.kinds[e.Path] = syscall.S_IFREG
		}
	}

	for p := range t.children {
		sort.Strings(t.children[p])
	}
	return t
}

// clearChildrenOf removes all entries that are direct children of the given directory.
func (t *overlayTree) clearChildrenOf(dir string) {
	prefix := dir + "/"
	if dir == "" {
		prefix = ""
	}
	for p := range t.entries {
		if dir == "" {
			// Root entries: anything without a "/" is a direct child of root.
			if !strings.Contains(p, "/") {
				delete(t.entries, p)
			}
		} else if strings.HasPrefix(p, prefix) {
			// Remove all descendants under this directory (OCI opaque semantics).
			delete(t.entries, p)
		}
	}
}

// removeEntry removes a single entry and all its descendants from the merged map.
func (t *overlayTree) removeEntry(p string) {
	delete(t.entries, p)
	// Also remove any descendants if this was a directory.
	prefix := p + "/"
	for k := range t.entries {
		if strings.HasPrefix(k, prefix) {
			delete(t.entries, k)
		}
	}
}

// OverlayDirNode is a directory in the unified overlay filesystem.
type OverlayDirNode struct {
	fs.Inode
	tree    *overlayTree
	relPath string
}

var _ = (fs.NodeLookuper)((*OverlayDirNode)(nil))
var _ = (fs.NodeReaddirer)((*OverlayDirNode)(nil))
var _ = (fs.NodeGetattrer)((*OverlayDirNode)(nil))

func (d *OverlayDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	t0 := time.Now()
	defer func() { d.tree.stats.LookupCount.Add(1); d.tree.stats.LookupNanos.Add(time.Since(t0).Nanoseconds()) }()
	if strings.Contains(name, "/") || name == "" {
		return nil, syscall.ENOENT
	}
	childPath := name
	if d.relPath != "" {
		childPath = d.relPath + "/" + name
	}
	d.tree.stats.RecordLookupPath(childPath)

	// RW mode: check deleted set first.
	if d.tree.upperDir != "" && d.tree.isDeleted(childPath) {
		return nil, syscall.ENOENT
	}

	// RW mode: check upper dir first (files created/modified at runtime).
	if d.tree.upperDir != "" {
		if c := d.GetChild(name); c != nil {
			realPath := d.tree.upperPath(childPath)
			if info, statErr := os.Lstat(realPath); statErr == nil {
				fillEntryOutFromFileInfo(out, info)
				if childModeMatchesFileInfo(c, info) {
					return c, 0
				}
				d.RmChild(name)
				child := d.newUpperChildInode(ctx, name, childPath, realPath, info)
				d.AddChild(name, child, true)
				return child, 0
			}
			d.RmChild(name)
		}
		if child, info := d.lookupInUpper(ctx, name, childPath); child != nil {
			d.AddChild(name, child, true)
			fillEntryOutFromFileInfo(out, info)
			return child, 0
		}
	}

	oe, ok := d.tree.entries[childPath]
	if !ok {
		return nil, syscall.ENOENT
	}
	setEntryOutAttr(out, oe.entry)
	if c := d.GetChild(name); c != nil {
		return c, 0
	}
	child := d.newChildNode(ctx, oe)
	d.AddChild(name, child, true)
	return child, 0
}

func (d *OverlayDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	t0 := time.Now()
	defer func() { d.tree.stats.ReaddirCount.Add(1); d.tree.stats.ReaddirNanos.Add(time.Since(t0).Nanoseconds()) }()

	// RW mode: merge upper dir entries with layer entries.
	if d.tree.upperDir != "" {
		return fs.NewListDirStream(d.mergedReaddir()), 0
	}

	names := d.tree.children[d.relPath]
	out := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		childPath := name
		if d.relPath != "" {
			childPath = d.relPath + "/" + name
		}
		mode := uint32(syscall.S_IFREG)
		if kind, ok := d.tree.kinds[childPath]; ok {
			mode = kind
		}
		out = append(out, fuse.DirEntry{Name: name, Mode: mode})
	}
	return fs.NewListDirStream(out), 0
}

func (d *OverlayDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	t0 := time.Now()
	defer func() { d.tree.stats.GetattrCount.Add(1); d.tree.stats.GetattrNanos.Add(time.Since(t0).Nanoseconds()) }()
	dirMode := uint32(0o555)
	if d.tree.upperDir != "" {
		dirMode = 0o755
	}
	out.Mode = uint32(syscall.S_IFDIR) | dirMode
	out.Nlink = 2 // runc checks Nlink>0 to verify directory is not deleted
	if d.relPath != "" {
		if oe, ok := d.tree.entries[d.relPath]; ok {
			setAttrTimes(out, oe.entry.ModTimeUnix)
		}
	}
	return 0
}

func (d *OverlayDirNode) newChildNode(ctx context.Context, oe overlayEntry) *fs.Inode {
	e := oe.entry
	switch e.Type {
	case layerformat.EntryTypeDir:
		node := &OverlayDirNode{tree: d.tree, relPath: e.Path}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR})
	case layerformat.EntryTypeSymlink:
		node := &SymlinkNode{entry: e, stats: d.tree.stats}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK})
	default:
		section, err := oe.reader.OpenFileSection(e.Path)
		if err != nil {
			section = layerformat.FileSection{}
		}
		node := &OverlayFileNode{
			entry:   e,
			section: section,
			store:   d.tree.store,
			stats:   d.tree.stats,
		}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	}
}

// OverlayFileNode is a read-only regular file in the unified overlay filesystem.
type OverlayFileNode struct {
	fs.Inode
	entry   layerformat.Entry
	section layerformat.FileSection
	store   *pagecache.Store
	stats   *FuseStats
}

var _ = (fs.NodeOpener)((*OverlayFileNode)(nil))
var _ = (fs.NodeReader)((*OverlayFileNode)(nil))
var _ = (fs.NodeGetattrer)((*OverlayFileNode)(nil))

func (f *OverlayFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	t0 := time.Now()
	defer func() { f.stats.OpenCount.Add(1); f.stats.OpenNanos.Add(time.Since(t0).Nanoseconds()) }()
	f.stats.RecordOpenPath(f.entry.Path)
	accessMode := flags & uint32(syscall.O_ACCMODE)
	if accessMode == syscall.O_WRONLY || accessMode == syscall.O_RDWR {
		return nil, 0, syscall.EROFS
	}
	if flags&uint32(syscall.O_APPEND) != 0 || flags&uint32(syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *OverlayFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	t0 := time.Now()
	defer func() {
		f.stats.ReadCount.Add(1)
		f.stats.ReadNanos.Add(time.Since(t0).Nanoseconds())
	}()
	fileSize := f.section.Size
	if off >= fileSize {
		return fuse.ReadResultData(nil), 0
	}
	remain := fileSize - off
	if remain > int64(len(dest)) {
		remain = int64(len(dest))
	}
	if remain <= 0 {
		return fuse.ReadResultData(nil), 0
	}
	buf := dest[:remain]

	if f.store != nil && f.entry.Digest != "" {
		n, err := f.store.ReadThrough(&f.section, f.entry.Digest, fileSize, buf, off)
		if err != nil && err != io.EOF {
			return nil, syscall.EIO
		}
		f.stats.ReadBytes.Add(int64(n))
		return fuse.ReadResultData(buf[:n]), 0
	}

	n, err := f.section.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	f.stats.ReadBytes.Add(int64(n))
	return fuse.ReadResultData(buf[:n]), 0
}

func (f *OverlayFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	t0 := time.Now()
	defer func() { f.stats.GetattrCount.Add(1); f.stats.GetattrNanos.Add(time.Since(t0).Nanoseconds()) }()
	out.Mode = uint32(syscall.S_IFREG | f.entry.Mode)
	out.Nlink = 1
	out.Size = uint64(f.section.Size)
	setAttrTimes(out, f.entry.ModTimeUnix)
	return 0
}
