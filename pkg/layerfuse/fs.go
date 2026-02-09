package layerfuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
)

// NewRoot builds a read-only inode tree backed by layerformat.Reader.
func NewRoot(r *layerformat.Reader) *DirNode {
	t := buildTree(r, "")
	return &DirNode{tree: t, relPath: ""}
}

// NewRootWithTempDir builds a read-only inode tree and uses tempDir to spill
// decompressed file payloads, avoiding full-file memory buffering.
func NewRootWithTempDir(r *layerformat.Reader, tempDir string) *DirNode {
	t := buildTree(r, tempDir)
	return &DirNode{tree: t, relPath: ""}
}

type tree struct {
	reader   *layerformat.Reader
	entries  map[string]layerformat.Entry
	children map[string][]string
	kinds    map[string]uint32
	tempDir  string
}

func buildTree(r *layerformat.Reader, tempDir string) *tree {
	t := &tree{
		reader:   r,
		entries:  make(map[string]layerformat.Entry),
		children: make(map[string][]string),
		kinds:    make(map[string]uint32),
		tempDir:  strings.TrimSpace(tempDir),
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
	if strings.Contains(name, "/") || name == "" {
		return nil, syscall.ENOENT
	}
	if c := d.GetChild(name); c != nil {
		return c, 0
	}
	childPath := name
	if d.relPath != "" {
		childPath = d.relPath + "/" + name
	}
	e, ok := d.tree.entries[childPath]
	if !ok {
		return nil, syscall.ENOENT
	}
	child := d.newChildNode(ctx, e)
	d.AddChild(name, child, true)
	return child, 0
}

func (d *DirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
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
	out.Mode = uint32(syscall.S_IFDIR | 0o555)
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
		node := &SymlinkNode{entry: e}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK})
	default:
		node := &FileNode{entry: e, reader: d.tree.reader, tempDir: d.tree.tempDir}
		return d.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG})
	}
}

// FileNode is a read-only regular file.
type FileNode struct {
	fs.Inode
	entry   layerformat.Entry
	reader  *layerformat.Reader
	tempDir string

	once sync.Once
	file *os.File
	size int64
	err  error
}

var _ = (fs.NodeOpener)((*FileNode)(nil))
var _ = (fs.NodeReader)((*FileNode)(nil))
var _ = (fs.NodeGetattrer)((*FileNode)(nil))

func (f *FileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	accessMode := flags & uint32(syscall.O_ACCMODE)
	if accessMode == syscall.O_WRONLY || accessMode == syscall.O_RDWR {
		return nil, 0, syscall.EROFS
	}
	if flags&uint32(syscall.O_APPEND) != 0 || flags&uint32(syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *FileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if errno := f.ensurePrepared(); errno != 0 {
		return nil, errno
	}
	if off >= f.size {
		return fuse.ReadResultData(nil), 0
	}
	buf := make([]byte, len(dest))
	n, err := f.file.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(buf[:n]), 0
}

func (f *FileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = uint32(syscall.S_IFREG | f.entry.Mode)
	if f.entry.UncompressedSize > 0 {
		out.Size = uint64(f.entry.UncompressedSize)
	} else {
		if errno := f.ensurePrepared(); errno != 0 {
			return errno
		}
		out.Size = uint64(f.size)
	}
	setAttrTimes(out, f.entry.ModTimeUnix)
	return 0
}

func (f *FileNode) ensurePrepared() syscall.Errno {
	f.once.Do(func() {
		f.file, f.size, f.err = f.prepareTempFile()
	})
	if f.err != nil {
		return syscall.EIO
	}
	return 0
}

func (f *FileNode) prepareTempFile() (*os.File, int64, error) {
	tmpDir := f.tempDir
	if tmpDir == "" {
		tmpDir = filepath.Join(os.TempDir(), "afs-fuse-spill")
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, 0, fmt.Errorf("create temp dir: %w", err)
	}
	finalPath := filepath.Join(tmpDir, spillFileName(f.entry))
	if st, err := os.Stat(finalPath); err == nil {
		fd, err := os.Open(finalPath)
		if err != nil {
			return nil, 0, err
		}
		return fd, st.Size(), nil
	}

	unlock := lockSpillPath(finalPath)
	defer unlock()
	if st, err := os.Stat(finalPath); err == nil {
		fd, err := os.Open(finalPath)
		if err != nil {
			return nil, 0, err
		}
		return fd, st.Size(), nil
	}

	tmp, err := os.CreateTemp(tmpDir, "afs-file-*")
	if err != nil {
		return nil, 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	n, err := f.reader.CopyFile(f.entry.Path, tmp)
	if err != nil {
		return nil, 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, 0, err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return nil, 0, err
	}
	fd, err := os.Open(finalPath)
	if err != nil {
		return nil, 0, err
	}
	return fd, n, nil
}

// SymlinkNode is a read-only symlink.
type SymlinkNode struct {
	fs.Inode
	entry layerformat.Entry
}

var _ = (fs.NodeReadlinker)((*SymlinkNode)(nil))
var _ = (fs.NodeGetattrer)((*SymlinkNode)(nil))

func (s *SymlinkNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte(s.entry.SymlinkTarget), 0
}

func (s *SymlinkNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
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

var (
	spillLocksMu sync.Mutex
	spillLocks   = make(map[string]*sync.Mutex)
)

func lockSpillPath(path string) func() {
	spillLocksMu.Lock()
	l, ok := spillLocks[path]
	if !ok {
		l = &sync.Mutex{}
		spillLocks[path] = l
	}
	spillLocksMu.Unlock()
	l.Lock()
	return l.Unlock
}

func spillFileName(e layerformat.Entry) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d|%d", e.Path, e.UncompressedSize, e.CompressedOffset, e.CompressedSize)))
	return hex.EncodeToString(sum[:]) + ".spill"
}
