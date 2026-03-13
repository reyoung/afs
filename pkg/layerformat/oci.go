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

// ConvertOCILayer converts a supported gzip-compressed tar layer into archive format.
// It produces AFSLYR01 (gzip-compressed payloads) by default.
func ConvertOCILayer(mediaType string, layer io.Reader, out io.Writer) error {
	if !IsSupportedLayerMediaType(mediaType) {
		return fmt.Errorf("unsupported media type: %s", mediaType)
	}
	return ConvertTarGzipToArchive(layer, out)
}

// ConvertOCILayerV2 converts a supported gzip-compressed tar layer into AFSLYR02 format
// with identity (plain) payloads.
func ConvertOCILayerV2(mediaType string, layer io.Reader, out io.Writer) error {
	if !IsSupportedLayerMediaType(mediaType) {
		return fmt.Errorf("unsupported media type: %s", mediaType)
	}
	return ConvertTarGzipToArchiveV2(layer, out)
}

// ConvertOCILayerWithVersion converts a supported gzip-compressed tar layer
// using the specified format version.
func ConvertOCILayerWithVersion(mediaType string, layer io.Reader, out io.Writer, version FormatVersion) error {
	switch version {
	case FormatV1:
		return ConvertOCILayer(mediaType, layer, out)
	case FormatV2:
		return ConvertOCILayerV2(mediaType, layer, out)
	default:
		return fmt.Errorf("unsupported format version: %d", version)
	}
}
