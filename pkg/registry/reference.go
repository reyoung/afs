package registry

import (
	"fmt"
	"strings"
)

const (
	defaultRegistry = "registry-1.docker.io"
	defaultTag      = "latest"
)

// ImageReference is a parsed docker image reference.
type ImageReference struct {
	Registry   string
	Repository string
	Reference  string // tag or digest
}

// ParseImageReference parses image name and tag into registry/repository/reference.
//
// Examples:
//   - ParseImageReference("nginx", "") => registry-1.docker.io/library/nginx:latest
//   - ParseImageReference("ghcr.io/org/app", "v1") => ghcr.io/org/app:v1
func ParseImageReference(image string, tag string) (ImageReference, error) {
	if strings.TrimSpace(image) == "" {
		return ImageReference{}, fmt.Errorf("image must not be empty")
	}

	var ref ImageReference
	name := strings.TrimSpace(image)

	if i := strings.LastIndex(name, "@"); i >= 0 {
		if tag != "" {
			return ImageReference{}, fmt.Errorf("tag cannot be set when image contains digest")
		}
		ref.Reference = name[i+1:]
		name = name[:i]
	}

	if ref.Reference == "" {
		lastSlash := strings.LastIndex(name, "/")
		lastColon := strings.LastIndex(name, ":")
		if tag == "" && lastColon > lastSlash {
			tag = name[lastColon+1:]
			name = name[:lastColon]
		}
		if tag == "" {
			tag = defaultTag
		}
		ref.Reference = tag
	}

	parts := strings.Split(name, "/")
	if len(parts) == 0 {
		return ImageReference{}, fmt.Errorf("invalid image name: %q", image)
	}

	if isRegistryComponent(parts[0]) {
		if len(parts) < 2 {
			return ImageReference{}, fmt.Errorf("invalid image name with registry: %q", image)
		}
		ref.Registry = normalizeRegistryHost(parts[0])
		ref.Repository = strings.Join(parts[1:], "/")
		// Docker Hub official images live under the implicit "library/" namespace.
		if ref.Registry == defaultRegistry && !strings.Contains(ref.Repository, "/") {
			ref.Repository = "library/" + ref.Repository
		}
	} else {
		ref.Registry = defaultRegistry
		ref.Repository = name
		if !strings.Contains(ref.Repository, "/") {
			ref.Repository = "library/" + ref.Repository
		}
	}

	if ref.Repository == "" || ref.Reference == "" {
		return ImageReference{}, fmt.Errorf("invalid image reference: %q", image)
	}
	return ref, nil
}

func isRegistryComponent(part string) bool {
	return strings.Contains(part, ".") || strings.Contains(part, ":") || part == "localhost"
}

func normalizeRegistryHost(host string) string {
	switch host {
	case "docker.io", "index.docker.io":
		return defaultRegistry
	default:
		return host
	}
}
