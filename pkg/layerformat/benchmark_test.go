package layerformat

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

func BenchmarkReaderCopyFile(b *testing.B) {
	const payloadSize = 8 << 20 // 8MiB
	payload := bytes.Repeat([]byte("a"), payloadSize)
	layer := buildBenchmarkTarGz(b, map[string][]byte{
		"bench/large.bin": payload,
	})

	var out bytes.Buffer
	if err := ConvertTarGzipToArchive(bytes.NewReader(layer), &out); err != nil {
		b.Fatalf("ConvertTarGzipToArchive() error = %v", err)
	}
	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		b.Fatalf("NewReader() error = %v", err)
	}

	b.SetBytes(payloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := r.CopyFile("bench/large.bin", io.Discard)
		if err != nil {
			b.Fatalf("CopyFile() error = %v", err)
		}
		if n != payloadSize {
			b.Fatalf("CopyFile() bytes=%d, want %d", n, payloadSize)
		}
	}
}

func buildBenchmarkTarGz(tb testing.TB, files map[string][]byte) []byte {
	tb.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	for name, data := range files {
		if err := tw.WriteHeader(&tar.Header{
			Name:     name,
			Typeflag: tar.TypeReg,
			Mode:     0o644,
			Size:     int64(len(data)),
		}); err != nil {
			tb.Fatalf("write tar header %s: %v", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			tb.Fatalf("write tar file %s: %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		tb.Fatalf("close tar writer: %v", err)
	}

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(tarBuf.Bytes()); err != nil {
		tb.Fatalf("write gzip: %v", err)
	}
	if err := gw.Close(); err != nil {
		tb.Fatalf("close gzip writer: %v", err)
	}
	return gzBuf.Bytes()
}
