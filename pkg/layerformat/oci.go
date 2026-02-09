package layerformat

import (
	"fmt"
	"io"
)

const OCILayerTarGzipMediaType = "application/vnd.oci.image.layer.v1.tar+gzip"

// ConvertOCILayer converts a layer with OCI media type
// application/vnd.oci.image.layer.v1.tar+gzip into archive format.
func ConvertOCILayer(mediaType string, layer io.Reader, out io.Writer) error {
	if mediaType != OCILayerTarGzipMediaType {
		return fmt.Errorf("unsupported media type: %s", mediaType)
	}
	return ConvertTarGzipToArchive(layer, out)
}
