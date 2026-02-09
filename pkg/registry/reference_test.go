package registry

import "testing"

func TestParseImageReference(t *testing.T) {
	tests := []struct {
		name      string
		image     string
		tag       string
		registry  string
		repo      string
		reference string
	}{
		{
			name:      "docker hub short name",
			image:     "nginx",
			tag:       "",
			registry:  "registry-1.docker.io",
			repo:      "library/nginx",
			reference: "latest",
		},
		{
			name:      "docker hub namespaced",
			image:     "org/app",
			tag:       "v1",
			registry:  "registry-1.docker.io",
			repo:      "org/app",
			reference: "v1",
		},
		{
			name:      "custom registry with tag in image",
			image:     "ghcr.io/acme/tool:v2",
			tag:       "",
			registry:  "ghcr.io",
			repo:      "acme/tool",
			reference: "v2",
		},
		{
			name:      "custom registry with explicit tag",
			image:     "localhost:5000/team/app",
			tag:       "release",
			registry:  "localhost:5000",
			repo:      "team/app",
			reference: "release",
		},
		{
			name:      "docker.io normalized with library prefix",
			image:     "docker.io/ubuntu:latest",
			tag:       "",
			registry:  "registry-1.docker.io",
			repo:      "library/ubuntu",
			reference: "latest",
		},
		{
			name:      "index.docker.io normalized with library prefix",
			image:     "index.docker.io/ubuntu",
			tag:       "",
			registry:  "registry-1.docker.io",
			repo:      "library/ubuntu",
			reference: "latest",
		},
		{
			name:      "digest reference",
			image:     "example.com/r/app@sha256:123",
			tag:       "",
			registry:  "example.com",
			repo:      "r/app",
			reference: "sha256:123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParseImageReference(tt.image, tt.tag)
			if err != nil {
				t.Fatalf("ParseImageReference() error = %v", err)
			}
			if ref.Registry != tt.registry || ref.Repository != tt.repo || ref.Reference != tt.reference {
				t.Fatalf("unexpected ref: got=%+v want={Registry:%s Repository:%s Reference:%s}", ref, tt.registry, tt.repo, tt.reference)
			}
		})
	}
}

func TestParseImageReference_Errors(t *testing.T) {
	_, err := ParseImageReference("", "")
	if err == nil {
		t.Fatal("expected error for empty image")
	}

	_, err = ParseImageReference("example.com/app@sha256:123", "v1")
	if err == nil {
		t.Fatal("expected error when both digest and tag are provided")
	}
}
