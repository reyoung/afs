package layerfuse

import (
	"context"
	"io"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/pagecache"
)

// NewRoot builds a read-only inode tree backed by layerformat.Reader.
func NewRoot(r *layerformat.Reader) (*DirNode, *FuseStats) {
	stats := &FuseStats{}
	t := buildTree(r, nil, stats)
	return &DirNode{tree: t, relPath: ""}, stats
}

// NewRootWithPageCache builds a read-only inode tree with page cache support.
func NewRootWithPageCache(r *layerformat.Reader, store *pagecache.Store) (*DirNode, *FuseStats) {
	stats := &FuseStats{}
	t := buildTree(r, store, stats)
	return &DirNode{tree: t, relPath: ""}, stats
}

type tree struct {
	reader   *layerformat.Reader
	store    *pagecache.Store
	stats    *FuseStats
	openless atomic.Bool
	entries  map[string]layerformat.Entry
	children map[string][]string
	kinds    map[string]uint32
}

func buildTree(r *layerformat.Reader, store *pagecache.Store, stats *FuseStats) *tree {
	t := &tree{
		reader:   r,
		store:    store,
		stats:    stats,
		entries:  make(map[string]layerformat.Entry),
		children: make(map[string][]string),
		kinds:    make(map[string]uint32),
	}
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
			t.entries[p] = layerformat.Entry{Path: p, Type: layerformat.EntryTypeDir, Mode: 0o755}
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

	for _, e := range r.Entries() {
		t.entries[e.Path] = e
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

// DirNode is a read-only directory node.
type DirNode struct {
	fs.Inode
	tree    *tree
	relPath string
}

var _ = (fs.NodeLookuper)((*DirNode)(nil))
var _ = (fs.NodeReaddirer)((*DirNode)(nil))
var _ = (fs.NodeGetattrer)((*DirNode)(nil))

func (d *DirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	t0 := time.Now()
	defer func() { d.tree.stats.LookupCount.Add(1); d.tree.stats.LookupNanos.Add(time.Since(t0).Nanoseconds()) }()
	if strings.Contains(name, "/") || name == "" {
		return nil, syscall.ENOENT
	}
	childPath := name
	if d.relPath != "" {
		childPath = d.relPath + "/" + name
	}
	e, ok := d.tree.entries[childPath]
	if !ok {
		return nil, syscall.ENOENT
	}
	setEntryOutAttr(out, e)
	if c := d.GetChild(name); c != nil {
		return c, 0
	}
	child := d.newChildNode(ctx, e)
	d.AddChild(name, child, true)
	return child, 0
}

func (d *DirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	t0 := time.Now()
	defer func() { d.tree.stats.ReaddirCount.Add(1); d.tree.stats.ReaddirNanos.Add(time.Since(t0).Nanoseconds()) }()
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

func (d *DirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	t0 := time.Now()
	defer func() { d.tree.stats.GetattrCount.Add(1); d.tree.stats.GetattrNanos.Add(time.Since(t0).Nanoseconds()) }()
	out.Mode = uint32(syscall.S_IFDIR | 0o555)
	out.Nlink = 2
	if d.relPath != "" {
		if e, ok := d.tree.entries[d.relPath]; ok {
			setAttrTimes(out, e.ModTimeUnix)
		}
	}
	return 0
}

func (d *DirNode) newChildNode(ctx context.Context, e layerformat.Entry) *fs.Inode {
	switch e.Type {
	case layerformat.EntryTypeDir:
		node := &DirNode{tree: d.tree, relPath: e.Path}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR})
	case layerformat.EntryTypeSymlink:
		node := &SymlinkNode{entry: e, stats: d.tree.stats}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK})
	default:
		section, err := d.tree.reader.OpenFileSection(e.Path)
		if err != nil {
			// Should not happen for valid archives; return a stub node.
			section = layerformat.FileSection{}
		}
		node := &FileNode{
			entry:    e,
			section:  section,
			store:    d.tree.store,
			stats:    d.tree.stats,
			openless: &d.tree.openless,
		}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	}
}

// FileNode is a read-only regular file backed by a direct archive section.
// When store is set, reads go through the in-process page cache.
type FileNode struct {
	fs.Inode
	entry    layerformat.Entry
	section  layerformat.FileSection
	store    *pagecache.Store
	stats    *FuseStats
	openless *atomic.Bool
}

var _ = (fs.NodeOpener)((*FileNode)(nil))
var _ = (fs.NodeReader)((*FileNode)(nil))
var _ = (fs.NodeGetattrer)((*FileNode)(nil))

func (f *FileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	t0 := time.Now()
	defer func() { f.stats.OpenCount.Add(1); f.stats.OpenNanos.Add(time.Since(t0).Nanoseconds()) }()
	accessMode := flags & uint32(syscall.O_ACCMODE)
	if accessMode == syscall.O_WRONLY || accessMode == syscall.O_RDWR {
		return nil, 0, syscall.EROFS
	}
	if flags&uint32(syscall.O_APPEND) != 0 || flags&uint32(syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	if f.openless != nil && f.openless.Load() {
		return nil, 0, syscall.ENOSYS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (d *DirNode) SetOpenless(enabled bool) {
	d.tree.openless.Store(enabled)
}

func (f *FileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
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

	// If page cache store is available, use ReadThrough for in-process caching.
	if f.store != nil && f.entry.Digest != "" {
		n, err := f.store.ReadThrough(&f.section, f.entry.Digest, fileSize, buf, off)
		if err != nil && err != io.EOF {
			return nil, syscall.EIO
		}
		f.stats.ReadBytes.Add(int64(n))
		f.stats.RecordReadPath(f.entry.Path, int64(n))
		return fuse.ReadResultData(buf[:n]), 0
	}

	// Direct read (no page cache).
	n, err := f.section.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	f.stats.ReadBytes.Add(int64(n))
	f.stats.RecordReadPath(f.entry.Path, int64(n))
	return fuse.ReadResultData(buf[:n]), 0
}

func (f *FileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	t0 := time.Now()
	defer func() { f.stats.GetattrCount.Add(1); f.stats.GetattrNanos.Add(time.Since(t0).Nanoseconds()) }()
	out.Mode = uint32(syscall.S_IFREG | f.entry.Mode)
	out.Nlink = 1
	out.Size = uint64(f.section.Size)
	setAttrTimes(out, f.entry.ModTimeUnix)
	return 0
}

// SymlinkNode is a read-only symlink.
type SymlinkNode struct {
	fs.Inode
	entry layerformat.Entry
	stats *FuseStats
}

var _ = (fs.NodeReadlinker)((*SymlinkNode)(nil))
var _ = (fs.NodeGetattrer)((*SymlinkNode)(nil))

func (s *SymlinkNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	t0 := time.Now()
	defer func() { s.stats.ReadlinkCount.Add(1); s.stats.ReadlinkNanos.Add(time.Since(t0).Nanoseconds()) }()
	s.stats.RecordReadlinkPath(s.entry.Path)
	return []byte(s.entry.SymlinkTarget), 0
}

func (s *SymlinkNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	t0 := time.Now()
	defer func() { s.stats.GetattrCount.Add(1); s.stats.GetattrNanos.Add(time.Since(t0).Nanoseconds()) }()
	out.Mode = uint32(syscall.S_IFLNK | 0o777)
	out.Size = uint64(len(s.entry.SymlinkTarget))
	setAttrTimes(out, s.entry.ModTimeUnix)
	return 0
}

func setAttrTimes(out *fuse.AttrOut, modTimeUnix int64) {
	if modTimeUnix <= 0 {
		return
	}
	ts := uint64(modTimeUnix)
	out.Atime = ts
	out.Mtime = ts
	out.Ctime = ts
}

func setEntryOutAttr(out *fuse.EntryOut, e layerformat.Entry) {
	switch e.Type {
	case layerformat.EntryTypeDir:
		mode := e.Mode
		if mode == 0 {
			mode = 0o755
		}
		out.Mode = uint32(syscall.S_IFDIR | mode)
		out.Nlink = 2
	case layerformat.EntryTypeSymlink:
		out.Mode = uint32(syscall.S_IFLNK | 0o777)
		out.Nlink = 1
		out.Size = uint64(len(e.SymlinkTarget))
	default:
		out.Mode = uint32(syscall.S_IFREG | e.Mode)
		out.Nlink = 1
		out.Size = uint64(e.UncompressedSize)
	}
	if e.ModTimeUnix > 0 {
		ts := uint64(e.ModTimeUnix)
		out.Atime = ts
		out.Mtime = ts
		out.Ctime = ts
	}
}
