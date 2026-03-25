package layerformat

import (
	"fmt"
	"io"
)

const OCILayerTarGzipMediaType = "application/vnd.oci.image.layer.v1.tar+gzip"
const DockerLayerTarGzipMediaType = "application/vnd.docker.image.rootfs.diff.tar.gzip"

// IsSupportedLayerMediaType reports whether this layer media type can be converted.
func IsSupportedLayerMediaType(mediaType string) bool {
	return mediaType == OCILayerTarGzipMediaType || mediaType == DockerLayerTarGzipMediaType
}

// ConvertOCILayer converts a supported gzip-compressed tar layer into AFSLYR02 format
// with identity (plain) payloads.
func ConvertOCILayer(mediaType string, layer io.Reader, out io.Writer) error {
	if !IsSupportedLayerMediaType(mediaType) {
		return fmt.Errorf("unsupported media type: %s", mediaType)
	}
	return ConvertTarGzipToArchive(layer, out)
}
