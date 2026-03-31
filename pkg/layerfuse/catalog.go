package layerfuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/filecache"
	"github.com/reyoung/afs/pkg/layerformat"
)

const ELFCacheDirName = "__elf_cache"

type CatalogRootNode struct {
	fs.Inode

	mu      sync.RWMutex
	names   map[string]struct{}
	elfDir  *CatalogELFDirNode
	elfNode *fs.Inode
}

type CatalogELFDirNode struct {
	fs.Inode

	mu       sync.RWMutex
	children map[string]*fs.Inode
	names    map[string]struct{}
}

type SharedELFFileNode struct {
	fs.Inode
	entry    layerformat.Entry
	section  layerformat.FileSection
	elfStore *filecache.Store
	openless *atomic.Bool
}

var _ = (fs.NodeLookuper)((*CatalogRootNode)(nil))
var _ = (fs.NodeReaddirer)((*CatalogRootNode)(nil))
var _ = (fs.NodeGetattrer)((*CatalogRootNode)(nil))
var _ = (fs.NodeLookuper)((*CatalogELFDirNode)(nil))
var _ = (fs.NodeReaddirer)((*CatalogELFDirNode)(nil))
var _ = (fs.NodeGetattrer)((*CatalogELFDirNode)(nil))
var _ = (fs.NodeOpener)((*SharedELFFileNode)(nil))
var _ = (fs.NodeReader)((*SharedELFFileNode)(nil))
var _ = (fs.NodeGetattrer)((*SharedELFFileNode)(nil))

func NewCatalogRoot() *CatalogRootNode {
	return &CatalogRootNode{
		names: make(map[string]struct{}),
	}
}

func (n *CatalogRootNode) ensureELFDir(ctx context.Context) *CatalogELFDirNode {
	if n.elfDir != nil {
		return n.elfDir
	}
	dir := &CatalogELFDirNode{
		children: make(map[string]*fs.Inode),
		names:    make(map[string]struct{}),
	}
	inode := n.NewPersistentInode(ctx, dir, fs.StableAttr{Mode: syscall.S_IFDIR})
	n.AddChild(ELFCacheDirName, inode, false)
	n.elfDir = dir
	n.elfNode = inode
	n.names[ELFCacheDirName] = struct{}{}
	return dir
}

func (n *CatalogRootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "" {
		return nil, syscall.ENOENT
	}
	ch := n.GetChild(name)
	if ch == nil {
		return nil, syscall.ENOENT
	}
	out.Mode = ch.StableAttr().Mode | 0o555
	out.Ino = ch.StableAttr().Ino
	out.NodeId = ch.StableAttr().Ino
	return ch, 0
}

func (n *CatalogRootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	n.mu.RLock()
	names := make([]string, 0, len(n.names))
	for name := range n.names {
		names = append(names, name)
	}
	n.mu.RUnlock()
	sort.Strings(names)
	out := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		out = append(out, fuse.DirEntry{Name: name, Mode: syscall.S_IFDIR})
	}
	return fs.NewListDirStream(out), 0
}

func (n *CatalogRootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0o555
	out.Nlink = 2
	return 0
}

func (n *CatalogRootNode) AddImage(ctx context.Context, name string, child *OverlayDirNode) (*fs.Inode, error) {
	if name == "" {
		return nil, syscall.EINVAL
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if existing := n.GetChild(name); existing != nil {
		n.names[name] = struct{}{}
		return existing, nil
	}
	inode := n.NewPersistentInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR})
	if !n.AddChild(name, inode, false) {
		if existing := n.GetChild(name); existing != nil {
			n.names[name] = struct{}{}
			return existing, nil
		}
		return nil, syscall.EEXIST
	}
	n.names[name] = struct{}{}
	_ = n.NotifyEntry(name)
	return inode, nil
}

func (n *CatalogRootNode) RemoveImage(name string) {
	if name == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := n.GetChild(name)
	if ch != nil {
		_, _ = n.RmChild(name)
		ch.ForgetPersistent()
		_ = n.NotifyDelete(name, ch)
	}
	delete(n.names, name)
}

func (n *CatalogRootNode) AddELF(ctx context.Context, digestName string, entry layerformat.Entry, section layerformat.FileSection, elfStore *filecache.Store, openless bool) (*fs.Inode, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dir := n.ensureELFDir(ctx)
	return dir.addELFChild(ctx, digestName, entry, section, elfStore, openless)
}

func (d *CatalogELFDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	d.mu.RLock()
	ch := d.children[name]
	d.mu.RUnlock()
	if ch == nil {
		return nil, syscall.ENOENT
	}
	out.Mode = ch.StableAttr().Mode | 0o444
	out.Ino = ch.StableAttr().Ino
	out.NodeId = ch.StableAttr().Ino
	return ch, 0
}

func (d *CatalogELFDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	d.mu.RLock()
	names := make([]string, 0, len(d.names))
	for name := range d.names {
		names = append(names, name)
	}
	d.mu.RUnlock()
	sort.Strings(names)
	out := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		out = append(out, fuse.DirEntry{Name: name, Mode: syscall.S_IFREG})
	}
	return fs.NewListDirStream(out), 0
}

func (d *CatalogELFDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0o555
	out.Nlink = 2
	return 0
}

func (d *CatalogELFDirNode) addELFChild(ctx context.Context, name string, entry layerformat.Entry, section layerformat.FileSection, elfStore *filecache.Store, openless bool) (*fs.Inode, error) {
	if name == "" {
		return nil, syscall.EINVAL
	}
	if existing := d.children[name]; existing != nil {
		return existing, nil
	}
	node := &SharedELFFileNode{
		entry:    entry,
		section:  section,
		elfStore: elfStore,
	}
	if openless {
		var v atomic.Bool
		v.Store(true)
		node.openless = &v
	}
	inode := d.NewPersistentInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	if !d.AddChild(name, inode, false) {
		if existing := d.GetChild(name); existing != nil {
			return existing, nil
		}
		return nil, syscall.EEXIST
	}
	d.children[name] = inode
	d.names[name] = struct{}{}
	_ = d.NotifyEntry(name)
	return inode, nil
}

func (f *SharedELFFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
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

func (f *SharedELFFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
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
	if f.elfStore != nil && f.entry.Digest != "" {
		n, err := f.elfStore.ReadThrough(&f.section, f.entry.Digest, fileSize, buf, off)
		if err != nil && err != io.EOF {
			logReadFailure("layerfuse.shared_elf_read", f.entry, f.section, off, len(buf), "elf-cache", err)
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(buf[:n]), 0
	}
	n, err := f.section.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		logReadFailure("layerfuse.shared_elf_read", f.entry, f.section, off, len(buf), "direct", err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(buf[:n]), 0
}

func (f *SharedELFFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = uint32(syscall.S_IFREG | f.entry.Mode)
	out.Nlink = 1
	out.Size = uint64(f.section.Size)
	setAttrTimes(out, f.entry.ModTimeUnix)
	return 0
}

func ELFCachePath(digestName string) string {
	return path.Join("/", ELFCacheDirName, digestName)
}

func sharedELFEntryName(e layerformat.Entry) string {
	digestName := strings.NewReplacer(":", "_", "/", "_").Replace(e.Digest)
	if digestName == "" {
		digestName = "elf"
	}
	attrSeed := fmt.Sprintf("%s|%d|%d|%d", e.Digest, e.Mode, e.ModTimeUnix, e.UncompressedSize)
	sum := sha256.Sum256([]byte(attrSeed))
	return digestName + "-" + hex.EncodeToString(sum[:4])
}
