package layerformat

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

func TestConvertTarGzipToArchive_ReadFileAndStat(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	if r.FormatVersion() != FormatV1 {
		t.Fatalf("expected FormatV1, got %d", r.FormatVersion())
	}

	e, err := r.Stat("dir/hello.txt")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if e.Type != EntryTypeFile {
		t.Fatalf("unexpected entry type: %s", e.Type)
	}

	data, err := r.ReadFile("dir/hello.txt")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

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
	if string(data) != "hello world" {
		t.Fatalf("unexpected file content: %q", string(data))
	}

	symlink, err := r.Stat("dir/link")
	if err != nil {
		t.Fatalf("Stat(symlink) error = %v", err)
	}
	if symlink.Type != EntryTypeSymlink || symlink.SymlinkTarget != "hello.txt" {
		t.Fatalf("unexpected symlink metadata: %+v", symlink)
	}

	hardlink, err := r.Stat("dir/hard")
	if err != nil {
		t.Fatalf("Stat(hardlink) error = %v", err)
	}
	if hardlink.Type != EntryTypeFile {
		t.Fatalf("unexpected hardlink entry type: %s", hardlink.Type)
	}
	hardlinkData, err := r.ReadFile("dir/hard")
	if err != nil {
		t.Fatalf("ReadFile(hardlink) error = %v", err)
	}
	if string(hardlinkData) != "hello world" {
		t.Fatalf("unexpected hardlink content: %q", string(hardlinkData))
	}
}

func TestV2RoundTrip(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchiveV2(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchiveV2() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	if r.FormatVersion() != FormatV2 {
		t.Fatalf("expected FormatV2, got %d", r.FormatVersion())
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

func TestV2HardlinkReusesPayload(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchiveV2(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchiveV2() error = %v", err)
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

	hardData, err := r.ReadFile("dir/hard")
	if err != nil {
		t.Fatalf("ReadFile(hard) error = %v", err)
	}
	if string(hardData) != "hello world" {
		t.Fatalf("unexpected hardlink content: %q", string(hardData))
	}
}

func TestV2OffsetRead(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchiveV2(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchiveV2() error = %v", err)
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

func TestV2OpenFileSection(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchiveV2(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchiveV2() error = %v", err)
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

func TestV1OpenFileSectionFails(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}

	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	_, err = r.OpenFileSection("dir/hello.txt")
	if err == nil {
		t.Fatal("expected error for V1 OpenFileSection")
	}
}

func TestV2MultipleFiles(t *testing.T) {
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
	if err := ConvertTarGzipToArchiveV2(bytes.NewReader(gzBuf.Bytes()), &out); err != nil {
		t.Fatalf("ConvertTarGzipToArchiveV2() error = %v", err)
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

func TestConvertOCILayerWithVersion(t *testing.T) {
	layer := buildLayerTarGz(t)

	// V1
	var v1Out bytes.Buffer
	if err := ConvertOCILayerWithVersion(OCILayerTarGzipMediaType, bytes.NewReader(layer), &v1Out, FormatV1); err != nil {
		t.Fatalf("ConvertOCILayerWithVersion(V1) error = %v", err)
	}
	if got := string(v1Out.Bytes()[:8]); got != "AFSLYR01" {
		t.Fatalf("V1 magic = %q, want AFSLYR01", got)
	}

	// V2
	var v2Out bytes.Buffer
	if err := ConvertOCILayerWithVersion(OCILayerTarGzipMediaType, bytes.NewReader(layer), &v2Out, FormatV2); err != nil {
		t.Fatalf("ConvertOCILayerWithVersion(V2) error = %v", err)
	}
	if got := string(v2Out.Bytes()[:8]); got != "AFSLYR02" {
		t.Fatalf("V2 magic = %q, want AFSLYR02", got)
	}

	// Invalid version
	var badOut bytes.Buffer
	if err := ConvertOCILayerWithVersion(OCILayerTarGzipMediaType, bytes.NewReader(layer), &badOut, 99); err == nil {
		t.Fatal("expected error for invalid format version")
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
