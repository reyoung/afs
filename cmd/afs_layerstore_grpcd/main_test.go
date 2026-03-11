package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseRegistryTokenPair(t *testing.T) {
	t.Parallel()

	host, token, err := parseRegistryTokenPair("registry-1.docker.io=abc123")
	if err != nil {
		t.Fatalf("parseRegistryTokenPair returned error: %v", err)
	}
	if host != "registry-1.docker.io" {
		t.Fatalf("host=%q, want %q", host, "registry-1.docker.io")
	}
	if token != "abc123" {
		t.Fatalf("token=%q, want %q", token, "abc123")
	}
}

func TestParseRegistryTokenPairInvalid(t *testing.T) {
	t.Parallel()

	if _, _, err := parseRegistryTokenPair("registry-1.docker.io"); err == nil {
		t.Fatalf("expected error for missing token")
	}
	if _, _, err := parseRegistryTokenPair("=abc123"); err == nil {
		t.Fatalf("expected error for missing host")
	}
}

func TestParseRegistryBasicPair(t *testing.T) {
	t.Parallel()

	host, username, password, err := parseRegistryBasicPair("registry-1.docker.io=user:pass")
	if err != nil {
		t.Fatalf("parseRegistryBasicPair returned error: %v", err)
	}
	if host != "registry-1.docker.io" {
		t.Fatalf("host=%q, want %q", host, "registry-1.docker.io")
	}
	if username != "user" {
		t.Fatalf("username=%q, want %q", username, "user")
	}
	if password != "pass" {
		t.Fatalf("password=%q, want %q", password, "pass")
	}
}

func TestParseRegistryBasicPairInvalid(t *testing.T) {
	t.Parallel()

	if _, _, _, err := parseRegistryBasicPair("ghcr.io"); err == nil {
		t.Fatalf("expected error for missing credentials")
	}
	if _, _, _, err := parseRegistryBasicPair("=alice"); err == nil {
		t.Fatalf("expected error for missing host")
	}
	if _, _, _, err := parseRegistryBasicPair("ghcr.io=:pass"); err == nil {
		t.Fatalf("expected error for missing username")
	}
}

func TestParseRegistryMirrorPair(t *testing.T) {
	t.Parallel()

	host, mirrors, err := parseRegistryMirrorPair("registry-1.docker.io=mirror.ccs.tencentyun.com, mirror.example.com")
	if err != nil {
		t.Fatalf("parseRegistryMirrorPair returned error: %v", err)
	}
	if host != "registry-1.docker.io" {
		t.Fatalf("host=%q, want %q", host, "registry-1.docker.io")
	}
	if len(mirrors) != 2 || mirrors[0] != "mirror.ccs.tencentyun.com" || mirrors[1] != "mirror.example.com" {
		t.Fatalf("mirrors=%v, want [mirror.ccs.tencentyun.com mirror.example.com]", mirrors)
	}
}

func TestParseRegistryMirrorPairInvalid(t *testing.T) {
	t.Parallel()

	if _, _, err := parseRegistryMirrorPair("registry-1.docker.io"); err == nil {
		t.Fatalf("expected error for missing mirror list")
	}
	if _, _, err := parseRegistryMirrorPair("=mirror.ccs.tencentyun.com"); err == nil {
		t.Fatalf("expected error for missing host")
	}
	if _, _, err := parseRegistryMirrorPair("registry-1.docker.io=, , "); err == nil {
		t.Fatalf("expected error for empty mirrors")
	}
}

func TestValidateListenEndpoint(t *testing.T) {
	t.Parallel()

	if err := validateListenEndpoint("10.0.0.1:50051"); err != nil {
		t.Fatalf("validateListenEndpoint() error: %v", err)
	}
	if err := validateListenEndpoint(":50051"); err == nil {
		t.Fatalf("expected error for missing ip")
	}
	if err := validateListenEndpoint("localhost:50051"); err == nil {
		t.Fatalf("expected error for non-ip host")
	}
}

func TestScanCachedLayerStats(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	layerPath := filepath.Join(tmp, "layers", "sha256", "abcdef.afslyr")
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(layerPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := scanCachedLayerStats(tmp)
	if err != nil {
		t.Fatalf("scanCachedLayerStats: %v", err)
	}
	if len(got) != 1 || got[0].GetDigest() != "sha256:abcdef" {
		t.Fatalf("got=%v, want digest=sha256:abcdef", got)
	}
	if got[0].GetAfsSize() != 1 {
		t.Fatalf("afs_size=%d, want 1", got[0].GetAfsSize())
	}
}

func TestScanCachedImageKeys_NoLayersField(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "metadata", "a.json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Old format without layers field — should not be reported (layers empty → skip)
	content := `{"image":"nginx","tag":"latest","platform_os":"linux","platform_arch":"amd64","platform_variant":""}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := scanCachedImageKeys(tmp)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got=%v, want [] (no layers field should not be reported)", got)
	}
}

func TestScanCachedImageKeys_LayersComplete(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	// Create metadata
	metaPath := filepath.Join(tmp, "metadata", "b.json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("MkdirAll metadata: %v", err)
	}
	content := `{"image":"nginx","tag":"latest","platform_os":"linux","platform_arch":"amd64","platform_variant":"","layers":[{"Digest":"sha256:abc123"}]}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile metadata: %v", err)
	}

	// Create the corresponding .afslyr file
	layerPath := filepath.Join(tmp, "layers", "sha256", "abc123.afslyr")
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatalf("MkdirAll layers: %v", err)
	}
	if err := os.WriteFile(layerPath, []byte("fake-layer-data"), 0o644); err != nil {
		t.Fatalf("WriteFile layer: %v", err)
	}

	got, err := scanCachedImageKeys(tmp)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 1 || got[0] != "nginx|latest|linux|amd64|" {
		t.Fatalf("got=%v, want [nginx|latest|linux|amd64|]", got)
	}
}

func TestScanCachedImageKeys_LayersMissing(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	// Create metadata with layers but do NOT create the .afslyr file
	metaPath := filepath.Join(tmp, "metadata", "c.json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("MkdirAll metadata: %v", err)
	}
	content := `{"image":"alpine","tag":"3.18","platform_os":"linux","platform_arch":"amd64","platform_variant":"","layers":[{"Digest":"sha256:deadbeef"}]}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile metadata: %v", err)
	}

	got, err := scanCachedImageKeys(tmp)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got=%v, want [] (missing layer file should not be reported)", got)
	}
}
