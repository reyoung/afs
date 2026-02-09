package layerformat

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
}

func buildLayerTarGz(t *testing.T) []byte {
	t.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	mustWriteTarHdr(t, tw, &tar.Header{Name: "dir", Typeflag: tar.TypeDir, Mode: 0o755})
	mustWriteTarFile(t, tw, "dir/hello.txt", []byte("hello world"), 0o644)
	mustWriteTarHdr(t, tw, &tar.Header{Name: "dir/link", Typeflag: tar.TypeSymlink, Linkname: "hello.txt", Mode: 0o777})

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
