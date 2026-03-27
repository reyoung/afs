package layerformat

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"io"
	"strconv"
	"testing"
)

type trackingReaderAt struct {
	ra         io.ReaderAt
	maxReadLen int
}

func (t *trackingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) > t.maxReadLen {
		t.maxReadLen = len(p)
	}
	return t.ra.ReadAt(p, off)
}

func TestRoundTrip(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	// Check magic
	if got := string(out.Bytes()[:8]); got != "AFSLYR02" {
		t.Fatalf("unexpected magic: %q", got)
	}

	// Read regular file
	data, err := r.ReadFile("dir/hello.txt")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("unexpected content: %q", string(data))
	}

	// Verify digest
	e, err := r.Stat("dir/hello.txt")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	wantSum := sha256.Sum256([]byte("hello world"))
	wantDigest := "sha256:" + hex.EncodeToString(wantSum[:])
	if e.Digest != wantDigest {
		t.Fatalf("Digest = %q, want %q", e.Digest, wantDigest)
	}
	if e.ContentKind != ContentKindUnknown {
		t.Fatalf("text file content metadata = %d, want unknown", e.ContentKind)
	}

	// CopyFile
	var copied bytes.Buffer
	n, err := r.CopyFile("dir/hello.txt", &copied)
	if err != nil {
		t.Fatalf("CopyFile() error = %v", err)
	}
	if n != int64(len("hello world")) {
		t.Fatalf("CopyFile() bytes = %d, want %d", n, len("hello world"))
	}
	if copied.String() != "hello world" {
		t.Fatalf("unexpected copied content: %q", copied.String())
	}

	// Symlink
	symlink, err := r.Stat("dir/link")
	if err != nil {
		t.Fatalf("Stat(symlink) error = %v", err)
	}
	if symlink.Type != EntryTypeSymlink || symlink.SymlinkTarget != "hello.txt" {
		t.Fatalf("unexpected symlink metadata: %+v", symlink)
	}

	// Dir
	dirEntry, err := r.Stat("dir")
	if err != nil {
		t.Fatalf("Stat(dir) error = %v", err)
	}
	if dirEntry.Type != EntryTypeDir {
		t.Fatalf("expected dir type, got %s", dirEntry.Type)
	}
}

func TestHardlinkReusesPayload(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	orig, err := r.Stat("dir/hello.txt")
	if err != nil {
		t.Fatalf("Stat(hello.txt) error = %v", err)
	}
	hard, err := r.Stat("dir/hard")
	if err != nil {
		t.Fatalf("Stat(hard) error = %v", err)
	}

	// Hardlink should share same payload metadata
	if hard.PayloadOffset != orig.PayloadOffset {
		t.Fatalf("hardlink PayloadOffset=%d, want %d", hard.PayloadOffset, orig.PayloadOffset)
	}
	if hard.PayloadSize != orig.PayloadSize {
		t.Fatalf("hardlink PayloadSize=%d, want %d", hard.PayloadSize, orig.PayloadSize)
	}
	if hard.PayloadCodec != PayloadCodecIdentity {
		t.Fatalf("hardlink PayloadCodec=%q, want %q", hard.PayloadCodec, PayloadCodecIdentity)
	}
	if hard.Digest != orig.Digest {
		t.Fatalf("hardlink Digest=%q, want %q", hard.Digest, orig.Digest)
	}
	if hard.Digest == "" {
		t.Fatal("hardlink Digest should not be empty")
	}

	hardData, err := r.ReadFile("dir/hard")
	if err != nil {
		t.Fatalf("ReadFile(hard) error = %v", err)
	}
	if string(hardData) != "hello world" {
		t.Fatalf("unexpected hardlink content: %q", string(hardData))
	}
}

func TestOffsetRead(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	// ReadFileAt partial read
	buf := make([]byte, 5)
	n, err := r.ReadFileAt("dir/hello.txt", buf, 6)
	if err != nil {
		t.Fatalf("ReadFileAt() error = %v", err)
	}
	if n != 5 {
		t.Fatalf("ReadFileAt() n = %d, want 5", n)
	}
	if string(buf[:n]) != "world" {
		t.Fatalf("ReadFileAt() = %q, want %q", string(buf[:n]), "world")
	}

	// ReadFileAt at beginning
	buf2 := make([]byte, 5)
	n2, err := r.ReadFileAt("dir/hello.txt", buf2, 0)
	if err != nil {
		t.Fatalf("ReadFileAt(0) error = %v", err)
	}
	if string(buf2[:n2]) != "hello" {
		t.Fatalf("ReadFileAt(0) = %q, want %q", string(buf2[:n2]), "hello")
	}

	// ReadFileAt past end
	buf3 := make([]byte, 10)
	_, err = r.ReadFileAt("dir/hello.txt", buf3, 100)
	if err != io.EOF {
		t.Fatalf("ReadFileAt past end: expected io.EOF, got %v", err)
	}
}

func TestOpenFileSection(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	section, err := r.OpenFileSection("dir/hello.txt")
	if err != nil {
		t.Fatalf("OpenFileSection() error = %v", err)
	}

	if section.Size != int64(len("hello world")) {
		t.Fatalf("section.Size=%d, want %d", section.Size, len("hello world"))
	}

	// Random read through section
	buf := make([]byte, 5)
	n, err := section.ReadAt(buf, 6)
	if err != nil && err != io.EOF {
		t.Fatalf("section.ReadAt() error = %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Fatalf("section.ReadAt() = %q, want %q", string(buf[:n]), "world")
	}
}

func TestNewReaderStreamsTOC(t *testing.T) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	for i := 0; i < 20000; i++ {
		name := "bin/file-" + strconv.Itoa(i)
		payload := buildMinimalELF64SharedObject(t)
		hdr := &tar.Header{
			Name: name,
			Mode: 0o755,
			Size: int64(len(payload)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader(%s) error = %v", name, err)
		}
		if _, err := tw.Write(payload); err != nil {
			t.Fatalf("Write(%s) error = %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close error = %v", err)
	}

	var gzBuf bytes.Buffer
	zw := gzip.NewWriter(&gzBuf)
	if _, err := zw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("gzip write error = %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close error = %v", err)
	}

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(gzBuf.Bytes()), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	tr := &trackingReaderAt{ra: bytes.NewReader(out.Bytes())}
	r, err := NewReader(tr)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	if len(r.Entries()) != 20000 {
		t.Fatalf("Entries()=%d, want 20000", len(r.Entries()))
	}
	if tr.maxReadLen >= len(out.Bytes())/2 {
		t.Fatalf("NewReader issued oversized ReadAt len=%d for archive size=%d", tr.maxReadLen, len(out.Bytes()))
	}
}

func TestMultipleFiles(t *testing.T) {
	// Build a layer with multiple files to test offset correctness
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	files := map[string]string{
		"a.txt": "alpha",
		"b.txt": "bravo-longer-content",
		"c.txt": "charlie",
	}

	mustWriteTarFile(t, tw, "a.txt", []byte("alpha"), 0o644)
	mustWriteTarFile(t, tw, "b.txt", []byte("bravo-longer-content"), 0o644)
	mustWriteTarFile(t, tw, "c.txt", []byte("charlie"), 0o644)

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write gzip: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(gzBuf.Bytes()), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	for name, expected := range files {
		data, err := r.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", name, err)
		}
		if string(data) != expected {
			t.Fatalf("ReadFile(%q) = %q, want %q", name, string(data), expected)
		}

		// Also verify offset read
		buf := make([]byte, len(expected))
		n, err := r.ReadFileAt(name, buf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadFileAt(%q) error = %v", name, err)
		}
		if string(buf[:n]) != expected {
			t.Fatalf("ReadFileAt(%q) = %q, want %q", name, string(buf[:n]), expected)
		}
	}
}

func TestConvertOCILayer(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertOCILayer(OCILayerTarGzipMediaType, bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertOCILayer() error = %v", err)
	}
	if got := string(out.Bytes()[:8]); got != "AFSLYR02" {
		t.Fatalf("magic = %q, want AFSLYR02", got)
	}

	// Invalid media type
	var badOut bytes.Buffer
	if err := ConvertOCILayer("application/invalid", bytes.NewReader(layer), &badOut); err == nil {
		t.Fatal("expected error for invalid media type")
	}
}

func TestConvertTarGzipToArchiveDetectsELFMetadata(t *testing.T) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	elfPayload := buildMinimalELF64SharedObject(t)
	mustWriteTarFile(t, tw, "usr/lib/libdemo.so", elfPayload, 0o644)

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write gzip: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(gzBuf.Bytes()), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	e, err := r.Stat("usr/lib/libdemo.so")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if e.ContentKind != ContentKindELF {
		t.Fatalf("ContentKind = %d, want %d", e.ContentKind, ContentKindELF)
	}
}

func buildLayerTarGz(t *testing.T) []byte {
	t.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	mustWriteTarHdr(t, tw, &tar.Header{Name: "dir", Typeflag: tar.TypeDir, Mode: 0o755})
	mustWriteTarFile(t, tw, "dir/hello.txt", []byte("hello world"), 0o644)
	mustWriteTarHdr(t, tw, &tar.Header{Name: "dir/link", Typeflag: tar.TypeSymlink, Linkname: "hello.txt", Mode: 0o777})
	mustWriteTarHdr(t, tw, &tar.Header{Name: "dir/hard", Typeflag: tar.TypeLink, Linkname: "hello.txt", Mode: 0o644})

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write gzip: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return gzBuf.Bytes()
}

func mustWriteTarFile(t *testing.T, tw *tar.Writer, name string, data []byte, mode int64) {
	t.Helper()
	mustWriteTarHdr(t, tw, &tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: mode, Size: int64(len(data))})
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("write tar file %s: %v", name, err)
	}
}

func mustWriteTarHdr(t *testing.T, tw *tar.Writer, hdr *tar.Header) {
	t.Helper()
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("write tar header %s: %v", hdr.Name, err)
	}
}

func buildMinimalELF64SharedObject(t *testing.T) []byte {
	t.Helper()

	const (
		ehdrSize = 64
		phdrSize = 56
		loadOff  = 0x1000
	)

	buf := make([]byte, loadOff+16)
	copy(buf[0:4], []byte{0x7f, 'E', 'L', 'F'})
	buf[4] = byte(elf.ELFCLASS64)
	buf[5] = byte(elf.ELFDATA2LSB)
	buf[6] = byte(elf.EV_CURRENT)
	buf[7] = byte(elf.ELFOSABI_NONE)

	put16 := func(off int, v uint16) { buf[off] = byte(v); buf[off+1] = byte(v >> 8) }
	put32 := func(off int, v uint32) {
		buf[off] = byte(v)
		buf[off+1] = byte(v >> 8)
		buf[off+2] = byte(v >> 16)
		buf[off+3] = byte(v >> 24)
	}
	put64 := func(off int, v uint64) {
		put32(off, uint32(v))
		put32(off+4, uint32(v>>32))
	}

	put16(16, uint16(elf.ET_DYN))
	put16(18, uint16(elf.EM_X86_64))
	put32(20, uint32(elf.EV_CURRENT))
	put64(24, 0)
	put64(32, ehdrSize)
	put64(40, 0)
	put32(48, 0)
	put16(52, ehdrSize)
	put16(54, phdrSize)
	put16(56, 1)
	put16(58, 0)
	put16(60, 0)
	put16(62, 0)

	ph := ehdrSize
	put32(ph+0, uint32(elf.PT_LOAD))
	put32(ph+4, uint32(elf.PF_R))
	put64(ph+8, loadOff)
	put64(ph+16, 0)
	put64(ph+24, 0)
	put64(ph+32, 16)
	put64(ph+40, 16)
	put64(ph+48, 0x1000)

	copy(buf[loadOff:], []byte("ELFTESTPAYLOAD!!"))
	return buf
}
