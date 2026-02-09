package main

import "testing"

func TestDefaultOutPath(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "a.tar.gz", want: "a.afslyr"},
		{in: "a.tgz", want: "a.afslyr"},
		{in: "layer.blob", want: "layer.blob.afslyr"},
	}

	for _, tt := range tests {
		if got := defaultOutPath(tt.in); got != tt.want {
			t.Fatalf("defaultOutPath(%q)=%q, want=%q", tt.in, got, tt.want)
		}
	}
}
