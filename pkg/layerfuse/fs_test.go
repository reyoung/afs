package layerfuse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/pagecache"
)

func TestSetAttrTimes(t *testing.T) {
	t.Parallel()

	out := &fuse.AttrOut{}
	setAttrTimes(out, 1700000000)

	if out.Mtime != 1700000000 {
		t.Fatalf("Mtime=%d, want 1700000000", out.Mtime)
	}
	if out.Atime != 1700000000 {
		t.Fatalf("Atime=%d, want 1700000000", out.Atime)
	}
	if out.Ctime != 1700000000 {
		t.Fatalf("Ctime=%d, want 1700000000", out.Ctime)
	}
}

func TestSetAttrTimesNonPositiveIgnored(t *testing.T) {
	t.Parallel()

	out := &fuse.AttrOut{}
	setAttrTimes(out, 0)
	if out.Mtime != 0 || out.Atime != 0 || out.Ctime != 0 {
		t.Fatalf("timestamps should stay zero, got atime=%d mtime=%d ctime=%d", out.Atime, out.Mtime, out.Ctime)
	}
}

func TestSetEntryOutAttrRegularFile(t *testing.T) {
	t.Parallel()

	out := &fuse.EntryOut{}
	setEntryOutAttr(out, layerformat.Entry{
		Type:             layerformat.EntryTypeFile,
		Mode:             0o755,
		UncompressedSize: 1234,
		ModTimeUnix:      1700000000,
	})

	if out.Mode != uint32(syscall.S_IFREG|0o755) {
		t.Fatalf("Mode=%o, want %o", out.Mode, uint32(syscall.S_IFREG|0o755))
	}
	if out.Size != 1234 {
		t.Fatalf("Size=%d, want 1234", out.Size)
	}
	if out.Mtime != 1700000000 {
		t.Fatalf("Mtime=%d, want 1700000000", out.Mtime)
	}
}

func newReaderWithSingleFile(t testing.TB, name string, data []byte) *layerformat.Reader {
	t.Helper()

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("write tar payload: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write gzip payload: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	var archive bytes.Buffer
	if err := layerformat.ConvertTarGzipToArchive(bytes.NewReader(gzBuf.Bytes()), &archive); err != nil {
		t.Fatalf("convert layer archive: %v", err)
	}
	reader, err := layerformat.NewReader(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("open layer archive reader: %v", err)
	}
	return reader
}

func TestFileNodeDirectRead(t *testing.T) {
	t.Parallel()

	const (
		filePath = "test.txt"
		content  = "hello-direct-read-from-archive"
	)
	reader := newReaderWithSingleFile(t, filePath, []byte(content))

	section, err := reader.OpenFileSection(filePath)
	if err != nil {
		t.Fatalf("OpenFileSection() error = %v", err)
	}

	node := &FileNode{
		entry:   layerformat.Entry{Path: filePath, Type: layerformat.EntryTypeFile, Mode: 0o644},
		section: section, stats: &FuseStats{},
	}

	// Test sequential read
	dest := make([]byte, len(content))
	ctx := context.TODO()
	result, errno := node.Read(ctx, nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read() errno = %d", errno)
	}
	buf, status := result.Bytes(dest)
	if status != fuse.OK {
		t.Fatalf("ReadResult.Bytes() status = %v", status)
	}
	if string(buf) != content {
		t.Fatalf("Read() = %q, want %q", string(buf), content)
	}

	// Test offset read
	dest2 := make([]byte, 10)
	result2, errno2 := node.Read(ctx, nil, dest2, 6)
	if errno2 != 0 {
		t.Fatalf("Read(off=6) errno = %d", errno2)
	}
	buf2, status2 := result2.Bytes(dest2)
	if status2 != fuse.OK {
		t.Fatalf("ReadResult.Bytes() status = %v", status2)
	}
	if string(buf2) != "direct-rea" {
		t.Fatalf("Read(off=6) = %q, want %q", string(buf2), "direct-rea")
	}

	// Test read past end
	dest3 := make([]byte, 10)
	result3, errno3 := node.Read(ctx, nil, dest3, int64(len(content)+10))
	if errno3 != 0 {
		t.Fatalf("Read(past end) errno = %d", errno3)
	}
	buf3, _ := result3.Bytes(dest3)
	if len(buf3) != 0 {
		t.Fatalf("Read(past end) should return empty, got %d bytes", len(buf3))
	}
}

func TestFileNodeGetattr(t *testing.T) {
	t.Parallel()

	const (
		filePath = "attr.txt"
		content  = "file-content-for-getattr"
	)
	reader := newReaderWithSingleFile(t, filePath, []byte(content))

	section, err := reader.OpenFileSection(filePath)
	if err != nil {
		t.Fatalf("OpenFileSection() error = %v", err)
	}

	node := &FileNode{
		entry:   layerformat.Entry{Path: filePath, Type: layerformat.EntryTypeFile, Mode: 0o644, UncompressedSize: int64(len(content))},
		section: section, stats: &FuseStats{},
	}

	out := &fuse.AttrOut{}
	errno := node.Getattr(context.TODO(), nil, out)
	if errno != 0 {
		t.Fatalf("Getattr() errno = %d", errno)
	}
	if out.Size != uint64(len(content)) {
		t.Fatalf("Getattr() Size=%d, want %d", out.Size, len(content))
	}
}

func TestFileNodeOpenKeepsCacheByDefault(t *testing.T) {
	t.Parallel()

	node := &FileNode{stats: &FuseStats{}}
	_, flags, errno := node.Open(context.TODO(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("Open() errno = %d", errno)
	}
	if flags != fuse.FOPEN_KEEP_CACHE {
		t.Fatalf("Open() flags = %#x, want %#x", flags, fuse.FOPEN_KEEP_CACHE)
	}
}

func TestFileNodeOpenlessReturnsENOSYS(t *testing.T) {
	t.Parallel()

	var openless atomic.Bool
	openless.Store(true)
	node := &FileNode{
		stats:    &FuseStats{},
		openless: &openless,
	}
	_, flags, errno := node.Open(context.TODO(), syscall.O_RDONLY)
	if errno != syscall.ENOSYS {
		t.Fatalf("Open() errno = %d, want %d", errno, syscall.ENOSYS)
	}
	if flags != 0 {
		t.Fatalf("Open() flags = %#x, want 0", flags)
	}
}

func TestOverlayFileNodeOpenlessReturnsENOSYS(t *testing.T) {
	t.Parallel()

	var openless atomic.Bool
	openless.Store(true)
	node := &OverlayFileNode{
		entry:    layerformat.Entry{Path: "test.txt"},
		stats:    &FuseStats{},
		openless: &openless,
	}
	_, flags, errno := node.Open(context.TODO(), syscall.O_RDONLY)
	if errno != syscall.ENOSYS {
		t.Fatalf("Open() errno = %d, want %d", errno, syscall.ENOSYS)
	}
	if flags != 0 {
		t.Fatalf("Open() flags = %#x, want 0", flags)
	}
}

func TestFileNodeReadUsesReadResultFdOnWarmSinglePageHit(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("warm-read-fd"), 1024)
	reader := newReaderWithSingleFile(t, "warm.bin", payload)
	section, err := reader.OpenFileSection("warm.bin")
	if err != nil {
		t.Fatalf("OpenFileSection() error = %v", err)
	}

	store, err := pagecache.NewStore(t.TempDir(), 8*pagecache.ChunkSize)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer store.Close()

	const digest = "sha256:warm-read-fd"
	warmDest := make([]byte, 4096)
	if _, err := store.ReadThrough(&section, digest, section.Size, warmDest, 0); err != nil {
		t.Fatalf("ReadThrough() error = %v", err)
	}

	node := &FileNode{
		entry: layerformat.Entry{
			Path:   "warm.bin",
			Type:   layerformat.EntryTypeFile,
			Mode:   0o644,
			Digest: digest,
		},
		section: section,
		store:   store,
		stats:   &FuseStats{},
	}

	dest := make([]byte, 4096)
	res, errno := node.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read() errno = %d", errno)
	}
	defer res.Done()

	if got := reflect.TypeOf(res).String(); got != "*fuse.readResultFd" {
		t.Fatalf("Read() type = %s, want *fuse.readResultFd", got)
	}

	data, status := res.Bytes(dest)
	if status != fuse.OK {
		t.Fatalf("ReadResult status = %v", status)
	}
	if !bytes.Equal(data, payload[:len(data)]) {
		t.Fatal("Read() returned unexpected bytes")
	}
}
