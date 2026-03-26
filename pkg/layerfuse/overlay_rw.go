package layerfuse

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
)

// WritableFileHandle is a file handle backed by a real *os.File in the upper dir.
type WritableFileHandle struct {
	f     *os.File
	stats *FuseStats
}

var _ = (fs.FileReader)((*WritableFileHandle)(nil))
var _ = (fs.FileWriter)((*WritableFileHandle)(nil))
var _ = (fs.FileFlusher)((*WritableFileHandle)(nil))
var _ = (fs.FileReleaser)((*WritableFileHandle)(nil))
var _ = (fs.FileGetattrer)((*WritableFileHandle)(nil))
var _ = (fs.FileSetattrer)((*WritableFileHandle)(nil))

func (h *WritableFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	t0 := time.Now()
	defer func() { h.stats.ReadCount.Add(1); h.stats.ReadNanos.Add(time.Since(t0).Nanoseconds()) }()
	n, err := h.f.ReadAt(dest, off)
	if n > 0 {
		h.stats.ReadBytes.Add(int64(n))
		return fuse.ReadResultData(dest[:n]), 0
	}
	if err != nil {
		log.Printf("WritableFileHandle.Read: off=%d len=%d n=%d err=%v file=%s", off, len(dest), n, err, h.f.Name())
		return fuse.ReadResultData(nil), 0
	}
	return fuse.ReadResultData(nil), 0
}

func (h *WritableFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	// Try WriteAt first; fall back to Seek+Write for O_APPEND files
	// (Go's os.File.WriteAt errors on O_APPEND files).
	n, err := h.f.WriteAt(data, off)
	if err != nil && strings.Contains(err.Error(), "O_APPEND") {
		// O_APPEND mode: use Write (kernel manages offset)
		n, err = h.f.Write(data)
	}
	if err != nil {
		log.Printf("WritableFileHandle.Write FAIL: off=%d len=%d n=%d err=%v file=%s", off, len(data), n, err, h.f.Name())
		return uint32(n), syscall.EIO
	}
	return uint32(n), 0
}

func (h *WritableFileHandle) Flush(ctx context.Context) syscall.Errno {
	if err := h.f.Sync(); err != nil {
		return syscall.EIO
	}
	return 0
}

func (h *WritableFileHandle) Release(ctx context.Context) syscall.Errno {
	_ = h.f.Close()
	return 0
}

func (h *WritableFileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	st, err := h.f.Stat()
	if err != nil {
		return syscall.EIO
	}
	fillAttrFromFileInfo(out, st)
	return 0
}

func (h *WritableFileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		if err := h.f.Truncate(int64(sz)); err != nil {
			return syscall.EIO
		}
	}
	if mode, ok := in.GetMode(); ok {
		if err := h.f.Chmod(os.FileMode(mode & 0o7777)); err != nil {
			return syscall.EIO
		}
	}
	return h.Getattr(ctx, out)
}

// WritableFileNode is a file node backed by a real file in the upper dir.
type WritableFileNode struct {
	fs.Inode
	realPath string
	stats    *FuseStats
}

var _ = (fs.NodeOpener)((*WritableFileNode)(nil))
var _ = (fs.NodeReader)((*WritableFileNode)(nil))
var _ = (fs.NodeGetattrer)((*WritableFileNode)(nil))
var _ = (fs.NodeSetattrer)((*WritableFileNode)(nil))

func (n *WritableFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	t0 := time.Now()
	defer func() { n.stats.OpenCount.Add(1); n.stats.OpenNanos.Add(time.Since(t0).Nanoseconds()) }()
	goFlags := int(flags) & (syscall.O_ACCMODE | syscall.O_APPEND | syscall.O_TRUNC)
	f, err := os.OpenFile(n.realPath, goFlags, 0)
	if err != nil {
		log.Printf("WritableFileNode.Open FAIL: path=%s flags=0x%x goFlags=0x%x err=%v", n.realPath, flags, goFlags, err)
		return nil, 0, syscall.EIO
	}
	log.Printf("WritableFileNode.Open OK: path=%s flags=0x%x goFlags=0x%x", n.realPath, flags, goFlags)
	return &WritableFileHandle{f: f, stats: n.stats}, 0, 0
}

func (n *WritableFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	t0 := time.Now()
	defer func() { n.stats.ReadCount.Add(1); n.stats.ReadNanos.Add(time.Since(t0).Nanoseconds()) }()
	f, err := os.Open(n.realPath)
	if err != nil {
		return nil, syscall.EIO
	}
	defer f.Close()
	n2, readErr := f.ReadAt(dest, off)
	if n2 > 0 {
		n.stats.ReadBytes.Add(int64(n2))
		return fuse.ReadResultData(dest[:n2]), 0
	}
	if readErr != nil {
		return fuse.ReadResultData(nil), 0
	}
	return fuse.ReadResultData(nil), 0
}

func (n *WritableFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	t0 := time.Now()
	defer func() { n.stats.GetattrCount.Add(1); n.stats.GetattrNanos.Add(time.Since(t0).Nanoseconds()) }()
	st, err := os.Lstat(n.realPath)
	if err != nil {
		return syscall.ENOENT
	}
	fillAttrFromFileInfo(out, st)
	return 0
}

func (n *WritableFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		if err := os.Truncate(n.realPath, int64(sz)); err != nil {
			return syscall.EIO
		}
	}
	if mode, ok := in.GetMode(); ok {
		if err := os.Chmod(n.realPath, os.FileMode(mode&0o7777)); err != nil {
			return syscall.EIO
		}
	}
	return n.Getattr(ctx, fh, out)
}

// WritableDirNode is a directory node backed by a real directory in the upper dir.
type WritableDirNode struct {
	fs.Inode
	realPath string
	tree     *overlayTree
	relPath  string
	stats    *FuseStats
}

var _ = (fs.NodeGetattrer)((*WritableDirNode)(nil))
var _ = (fs.NodeSetattrer)((*WritableDirNode)(nil))
var _ = (fs.NodeLookuper)((*WritableDirNode)(nil))
var _ = (fs.NodeReaddirer)((*WritableDirNode)(nil))
var _ = (fs.NodeCreater)((*WritableDirNode)(nil))
var _ = (fs.NodeMkdirer)((*WritableDirNode)(nil))
var _ = (fs.NodeUnlinker)((*WritableDirNode)(nil))
var _ = (fs.NodeRmdirer)((*WritableDirNode)(nil))
var _ = (fs.NodeRenamer)((*WritableDirNode)(nil))
var _ = (fs.NodeSymlinker)((*WritableDirNode)(nil))

func (d *WritableDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	st, err := os.Lstat(d.realPath)
	if err != nil {
		return syscall.ENOENT
	}
	fillAttrFromFileInfo(out, st)
	return 0
}

func (d *WritableDirNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if mode, ok := in.GetMode(); ok {
		if err := os.Chmod(d.realPath, os.FileMode(mode&0o7777)); err != nil {
			return syscall.EIO
		}
	}
	return d.Getattr(ctx, fh, out)
}

func (d *WritableDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if strings.Contains(name, "/") || name == "" {
		return nil, syscall.ENOENT
	}
	childReal := filepath.Join(d.realPath, name)
	st, err := os.Lstat(childReal)
	if err != nil {
		return nil, syscall.ENOENT
	}
	fillEntryOutFromFileInfo(out, st)
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	if c := d.GetChild(name); c != nil {
		return c, 0
	}
	child := d.makeChildInode(ctx, childReal, childRel, st)
	d.AddChild(name, child, true)
	return child, 0
}

func (d *WritableDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := os.ReadDir(d.realPath)
	if err != nil {
		return nil, syscall.EIO
	}
	out := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir() {
			mode = syscall.S_IFDIR
		} else if e.Type()&os.ModeSymlink != 0 {
			mode = syscall.S_IFLNK
		}
		out = append(out, fuse.DirEntry{Name: e.Name(), Mode: mode})
	}
	return fs.NewListDirStream(out), 0
}

func (d *WritableDirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	childReal := filepath.Join(d.realPath, name)
	f, err := os.OpenFile(childReal, int(flags)|os.O_CREATE, os.FileMode(mode&0o7777))
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	node := &WritableFileNode{realPath: childReal, stats: d.stats}
	child := d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	d.AddChild(name, child, true)
	fh := &WritableFileHandle{f: f, stats: d.stats}
	return child, fh, 0, 0
}

func (d *WritableDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childReal := filepath.Join(d.realPath, name)
	if err := os.Mkdir(childReal, os.FileMode(mode&0o7777)); err != nil {
		return nil, syscall.EIO
	}
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	node := &WritableDirNode{realPath: childReal, tree: d.tree, relPath: childRel, stats: d.stats}
	child := d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR})
	d.AddChild(name, child, true)
	return child, 0
}

func (d *WritableDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childReal := filepath.Join(d.realPath, name)
	if err := os.Remove(childReal); err != nil {
		return syscall.EIO
	}
	return 0
}

func (d *WritableDirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childReal := filepath.Join(d.realPath, name)
	if err := os.Remove(childReal); err != nil {
		return syscall.EIO
	}
	return 0
}

func (d *WritableDirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := filepath.Join(d.realPath, name)
	var newDir string
	switch np := newParent.(type) {
	case *WritableDirNode:
		newDir = np.realPath
	case *OverlayDirNode:
		if np.tree.upperDir == "" {
			return syscall.EROFS
		}
		newDir = np.tree.upperPath(np.relPath)
	default:
		return syscall.ENOTSUP
	}
	newPath := filepath.Join(newDir, newName)
	if err := os.Rename(oldPath, newPath); err != nil {
		return syscall.EIO
	}
	return 0
}

func (d *WritableDirNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childReal := filepath.Join(d.realPath, name)
	if err := os.Symlink(target, childReal); err != nil {
		return nil, syscall.EIO
	}
	node := &SymlinkNode{
		entry: newRWSymlinkEntry(name, d.relPath, target),
		stats: d.stats,
	}
	child := d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK})
	d.AddChild(name, child, true)
	return child, 0
}

func (d *WritableDirNode) makeChildInode(ctx context.Context, realPath, relPath string, info os.FileInfo) *fs.Inode {
	if info.IsDir() {
		node := &WritableDirNode{realPath: realPath, tree: d.tree, relPath: relPath, stats: d.stats}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR})
	}
	if info.Mode()&os.ModeSymlink != 0 {
		target, _ := os.Readlink(realPath)
		node := &SymlinkNode{
			entry: newRWSymlinkEntry(filepath.Base(relPath), filepath.Dir(relPath), target),
			stats: d.stats,
		}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK})
	}
	node := &WritableFileNode{realPath: realPath, stats: d.stats}
	return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
}

// --- OverlayDirNode write operations ---

// Compile-time checks for write interfaces on OverlayDirNode.
var _ = (fs.NodeCreater)((*OverlayDirNode)(nil))
var _ = (fs.NodeMkdirer)((*OverlayDirNode)(nil))
var _ = (fs.NodeUnlinker)((*OverlayDirNode)(nil))
var _ = (fs.NodeRmdirer)((*OverlayDirNode)(nil))
var _ = (fs.NodeRenamer)((*OverlayDirNode)(nil))
var _ = (fs.NodeSymlinker)((*OverlayDirNode)(nil))
var _ = (fs.NodeLinker)((*OverlayDirNode)(nil))
var _ = (fs.NodeSetattrer)((*OverlayDirNode)(nil))

func (d *OverlayDirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if d.tree.upperDir == "" {
		return nil, nil, 0, syscall.EROFS
	}
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	realPath := d.tree.upperPath(childRel)
	if err := os.MkdirAll(filepath.Dir(realPath), 0o755); err != nil {
		return nil, nil, 0, syscall.EIO
	}
	f, err := os.OpenFile(realPath, int(flags)|os.O_CREATE, os.FileMode(mode&0o7777))
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	// Remove from deleted set if it was previously deleted.
	d.tree.mu.Lock()
	delete(d.tree.deleted, childRel)
	d.tree.mu.Unlock()

	node := &WritableFileNode{realPath: realPath, stats: d.tree.stats}
	child := d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	d.AddChild(name, child, true)
	fh := &WritableFileHandle{f: f, stats: d.tree.stats}
	return child, fh, 0, 0
}

func (d *OverlayDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.tree.upperDir == "" {
		return nil, syscall.EROFS
	}
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	realPath := d.tree.upperPath(childRel)
	if err := os.MkdirAll(filepath.Dir(realPath), 0o755); err != nil {
		return nil, syscall.EIO
	}
	if err := os.Mkdir(realPath, os.FileMode(mode&0o7777)); err != nil {
		return nil, syscall.EIO
	}

	d.tree.mu.Lock()
	delete(d.tree.deleted, childRel)
	d.tree.mu.Unlock()

	node := &WritableDirNode{realPath: realPath, tree: d.tree, relPath: childRel, stats: d.tree.stats}
	child := d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR})
	d.AddChild(name, child, true)
	return child, 0
}

func (d *OverlayDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if d.tree.upperDir == "" {
		return syscall.EROFS
	}
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	// Try removing from upper dir first.
	upperPath := d.tree.upperPath(childRel)
	if _, err := os.Lstat(upperPath); err == nil {
		if err := os.Remove(upperPath); err != nil {
			return syscall.EIO
		}
	}
	// If it exists in merged layers, mark as deleted (whiteout).
	if _, ok := d.tree.entries[childRel]; ok {
		d.tree.markDeleted(childRel)
	}
	d.RmChild(name)
	return 0
}

func (d *OverlayDirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if d.tree.upperDir == "" {
		return syscall.EROFS
	}
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	upperPath := d.tree.upperPath(childRel)
	if _, err := os.Lstat(upperPath); err == nil {
		if err := os.Remove(upperPath); err != nil {
			return syscall.EIO
		}
	}
	if _, ok := d.tree.entries[childRel]; ok {
		d.tree.markDeleted(childRel)
	}
	d.RmChild(name)
	return 0
}

func (d *OverlayDirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if d.tree.upperDir == "" {
		return syscall.EROFS
	}
	oldRel := name
	if d.relPath != "" {
		oldRel = d.relPath + "/" + name
	}

	// Figure out new parent's relPath and determine new upper path.
	var newRel string
	switch np := newParent.(type) {
	case *OverlayDirNode:
		newRel = newName
		if np.relPath != "" {
			newRel = np.relPath + "/" + newName
		}
	case *WritableDirNode:
		newRel = newName
		if np.relPath != "" {
			newRel = np.relPath + "/" + newName
		}
	default:
		return syscall.ENOTSUP
	}

	// Ensure source exists in upper (may need copy-up).
	oldUpper := d.tree.upperPath(oldRel)
	newUpper := d.tree.upperPath(newRel)
	if err := os.MkdirAll(filepath.Dir(newUpper), 0o755); err != nil {
		return syscall.EIO
	}

	// If the file exists in upper, rename it.
	if _, err := os.Lstat(oldUpper); err == nil {
		if err := os.Rename(oldUpper, newUpper); err != nil {
			return syscall.EIO
		}
	}

	// Mark old entry as deleted if it was from merged layers.
	if _, ok := d.tree.entries[oldRel]; ok {
		d.tree.markDeleted(oldRel)
	}

	// Unmark new path from deleted set.
	d.tree.mu.Lock()
	delete(d.tree.deleted, newRel)
	d.tree.mu.Unlock()

	return 0
}

func (d *OverlayDirNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.tree.upperDir == "" {
		return nil, syscall.EROFS
	}
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	realPath := d.tree.upperPath(childRel)
	if err := os.MkdirAll(filepath.Dir(realPath), 0o755); err != nil {
		return nil, syscall.EIO
	}
	if err := os.Symlink(target, realPath); err != nil {
		return nil, syscall.EIO
	}

	d.tree.mu.Lock()
	delete(d.tree.deleted, childRel)
	d.tree.mu.Unlock()

	node := &SymlinkNode{
		entry: newRWSymlinkEntry(name, d.relPath, target),
		stats: d.tree.stats,
	}
	child := d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK})
	d.AddChild(name, child, true)
	return child, 0
}

func (d *OverlayDirNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.tree.upperDir == "" {
		return nil, syscall.EROFS
	}
	var targetRealPath string
	switch t := target.(type) {
	case *WritableFileNode:
		targetRealPath = t.realPath
	default:
		return nil, syscall.ENOTSUP
	}
	childRel := name
	if d.relPath != "" {
		childRel = d.relPath + "/" + name
	}
	realPath := d.tree.upperPath(childRel)
	if err := os.MkdirAll(filepath.Dir(realPath), 0o755); err != nil {
		return nil, syscall.EIO
	}
	if err := os.Link(targetRealPath, realPath); err != nil {
		return nil, syscall.EIO
	}

	d.tree.mu.Lock()
	delete(d.tree.deleted, childRel)
	d.tree.mu.Unlock()

	node := &WritableFileNode{realPath: realPath, stats: d.tree.stats}
	child := d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	d.AddChild(name, child, true)
	return child, 0
}

func (d *OverlayDirNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if d.tree.upperDir == "" {
		return syscall.EROFS
	}
	realPath := d.tree.upperPath(d.relPath)
	if err := os.MkdirAll(realPath, 0o755); err != nil {
		return syscall.EIO
	}
	if mode, ok := in.GetMode(); ok {
		if err := os.Chmod(realPath, os.FileMode(mode&0o7777)); err != nil {
			return syscall.EIO
		}
	}
	return d.Getattr(ctx, fh, out)
}

// --- overlayTree helper methods for RW support ---

// upperPath returns the full path in the upper dir for a given relative path.
func (t *overlayTree) upperPath(relPath string) string {
	if relPath == "" {
		return t.upperDir
	}
	return filepath.Join(t.upperDir, relPath)
}

// isDeleted checks if a path has been deleted.
func (t *overlayTree) isDeleted(relPath string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.deleted[relPath]
	return ok
}

// markDeleted marks a path as deleted.
func (t *overlayTree) markDeleted(relPath string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleted[relPath] = struct{}{}
}

// existsInUpper checks if a file exists in the upper dir.
func (t *overlayTree) existsInUpper(relPath string) bool {
	if t.upperDir == "" {
		return false
	}
	_, err := os.Lstat(t.upperPath(relPath))
	return err == nil
}

// --- helpers ---

func fillAttrFromFileInfo(out *fuse.AttrOut, info os.FileInfo) {
	out.Size = uint64(info.Size())
	out.Mode = fileInfoToFuseMode(info)
	out.Nlink = 1
	if info.IsDir() {
		out.Nlink = 2
	}
	ts := uint64(info.ModTime().Unix())
	out.Atime = ts
	out.Mtime = ts
	out.Ctime = ts
}

func fillEntryOutFromFileInfo(out *fuse.EntryOut, info os.FileInfo) {
	out.Size = uint64(info.Size())
	out.Mode = fileInfoToFuseMode(info)
	out.Nlink = 1
	if info.IsDir() {
		out.Nlink = 2
	}
	ts := uint64(info.ModTime().Unix())
	out.Atime = ts
	out.Mtime = ts
	out.Ctime = ts
}

func fileInfoToFuseMode(info os.FileInfo) uint32 {
	perm := uint32(info.Mode().Perm())
	if info.IsDir() {
		return syscall.S_IFDIR | perm
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return syscall.S_IFLNK | 0o777
	}
	return syscall.S_IFREG | perm
}

func newRWSymlinkEntry(name, parentRel, target string) layerformat.Entry {
	p := joinRelPath(parentRel, name)
	return layerformat.Entry{
		Path:          p,
		Type:          layerformat.EntryTypeSymlink,
		Mode:          0o777,
		SymlinkTarget: target,
	}
}

func joinRelPath(parent, name string) string {
	if parent == "" || parent == "." {
		return name
	}
	return parent + "/" + name
}

// lookupInUpper checks the upper dir for a child and returns an appropriate inode.
// Returns nil if the child does not exist in the upper dir.
func (d *OverlayDirNode) lookupInUpper(ctx context.Context, name, childRel string) (*fs.Inode, os.FileInfo) {
	if d.tree.upperDir == "" {
		return nil, nil
	}
	realPath := d.tree.upperPath(childRel)
	info, err := os.Lstat(realPath)
	if err != nil {
		return nil, nil
	}
	if info.IsDir() {
		node := &WritableDirNode{realPath: realPath, tree: d.tree, relPath: childRel, stats: d.tree.stats}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR}), info
	}
	if info.Mode()&os.ModeSymlink != 0 {
		target, _ := os.Readlink(realPath)
		node := &SymlinkNode{
			entry: newRWSymlinkEntry(name, d.relPath, target),
			stats: d.tree.stats,
		}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK}), info
	}
	node := &WritableFileNode{realPath: realPath, stats: d.tree.stats}
	return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG}), info
}

// readUpperDirEntries returns directory entries from the upper dir for the given relPath.
func (d *OverlayDirNode) readUpperDirEntries() []fuse.DirEntry {
	if d.tree.upperDir == "" {
		return nil
	}
	realPath := d.tree.upperPath(d.relPath)
	entries, err := os.ReadDir(realPath)
	if err != nil {
		return nil
	}
	out := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir() {
			mode = syscall.S_IFDIR
		} else if e.Type()&os.ModeSymlink != 0 {
			mode = syscall.S_IFLNK
		}
		out = append(out, fuse.DirEntry{Name: e.Name(), Mode: mode})
	}
	return out
}

// mergedReaddir returns a merged view of upper dir + layer entries, excluding deleted.
func (d *OverlayDirNode) mergedReaddir() []fuse.DirEntry {
	seen := make(map[string]struct{})
	var out []fuse.DirEntry

	// Upper dir entries first (they take precedence).
	for _, e := range d.readUpperDirEntries() {
		childRel := e.Name
		if d.relPath != "" {
			childRel = d.relPath + "/" + e.Name
		}
		if d.tree.isDeleted(childRel) {
			continue
		}
		seen[e.Name] = struct{}{}
		out = append(out, e)
	}

	// Layer entries.
	names := d.tree.children[d.relPath]
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		childRel := name
		if d.relPath != "" {
			childRel = d.relPath + "/" + name
		}
		if d.tree.isDeleted(childRel) {
			continue
		}
		mode := uint32(syscall.S_IFREG)
		if kind, ok := d.tree.kinds[childRel]; ok {
			mode = kind
		}
		out = append(out, fuse.DirEntry{Name: name, Mode: mode})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
