package layerformat

import (
	"bytes"
	"testing"
)

func TestConvertOCILayerSupportsDockerLayerMediaType(t *testing.T) {
	layer := buildLayerTarGz(t)

	var out bytes.Buffer
	if err := ConvertOCILayer(DockerLayerTarGzipMediaType, bytes.NewReader(layer), &out); err != nil {
		t.Fatalf("ConvertOCILayer() error = %v", err)
	}
	if out.Len() < 8 {
		t.Fatalf("converted output too short: %d", out.Len())
	}
	if got := string(out.Bytes()[:8]); got != "AFSLYR01" {
		t.Fatalf("unexpected archive magic: %q", got)
	}
}

func TestConvertOCILayerRejectsUnsupportedMediaType(t *testing.T) {
	var out bytes.Buffer
	err := ConvertOCILayer("application/octet-stream", bytes.NewReader(nil), &out)
	if err == nil {
		t.Fatal("expected error for unsupported media type")
	}
}
