package afslet

import "testing"

func TestNormalizeImageAndTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		image     string
		tag       string
		wantImage string
		wantTag   string
		wantErr   bool
	}{
		{name: "image only", image: "alpine", tag: "", wantImage: "alpine", wantTag: "", wantErr: false},
		{name: "split image tag", image: "alpine:latest", tag: "", wantImage: "alpine", wantTag: "latest", wantErr: false},
		{name: "duplicate same tag", image: "alpine:latest", tag: "latest", wantImage: "alpine", wantTag: "latest", wantErr: false},
		{name: "registry port no tag", image: "127.0.0.1:5000/alpine", tag: "", wantImage: "127.0.0.1:5000/alpine", wantTag: "", wantErr: false},
		{name: "mismatch tag", image: "alpine:3.19", tag: "latest", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			img, tag, err := normalizeImageAndTag(tc.image, tc.tag)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if img != tc.wantImage || tag != tc.wantTag {
				t.Fatalf("got image=%q tag=%q, want image=%q tag=%q", img, tag, tc.wantImage, tc.wantTag)
			}
		})
	}
}
