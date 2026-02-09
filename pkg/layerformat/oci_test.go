package layerformat

import (
	"bytes"
	"testing"
)

func TestConvertOCILayerRejectsUnsupportedMediaType(t *testing.T) {
	var out bytes.Buffer
	err := ConvertOCILayer("application/octet-stream", bytes.NewReader(nil), &out)
	if err == nil {
		t.Fatal("expected error for unsupported media type")
	}
}
