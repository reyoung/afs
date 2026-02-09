package registry

// Descriptor describes an OCI/Docker object such as config or layer.
type Descriptor struct {
	MediaType   string            `json:"mediaType"`
	Size        int64             `json:"size"`
	Digest      string            `json:"digest"`
	URLs        []string          `json:"urls,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// Manifest describes a single image manifest.
type Manifest struct {
	SchemaVersion int        `json:"schemaVersion"`
	MediaType     string     `json:"mediaType"`
	Config        Descriptor `json:"config"`
	Layers        []Descriptor `json:"layers"`
}

// Platform describes os/arch for a manifest entry in a manifest list/index.
type Platform struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Variant      string `json:"variant,omitempty"`
}

// ManifestListEntry is a single entry in docker manifest list / OCI index.
type ManifestListEntry struct {
	Descriptor
	Platform Platform `json:"platform"`
}

// ManifestList is a multi-platform image index.
type ManifestList struct {
	SchemaVersion int                 `json:"schemaVersion"`
	MediaType     string              `json:"mediaType"`
	Manifests     []ManifestListEntry `json:"manifests"`
}

// Layer contains flattened metadata used by callers.
type Layer struct {
	Digest    string
	MediaType string
	Size      int64
}
