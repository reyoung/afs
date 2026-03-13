package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/reyoung/afs/pkg/layerformat"
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

	got, err := scanCachedLayerStats(tmp, layerformat.FormatV1)
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
	content := `{"image":"nginx","tag":"latest","platform_os":"linux","platform_arch":"amd64","platform_variant":""}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := scanCachedImageKeys(tmp, layerformat.FormatV1)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got=%v, want []", got)
	}
}

func TestScanCachedImageKeys_LayersComplete(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "metadata", "a.json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := `{"image":"nginx","tag":"latest","platform_os":"linux","platform_arch":"amd64","platform_variant":"","layers":[{"Digest":"sha256:abcdef"}]}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	layerPath := filepath.Join(tmp, "layers", "sha256", "abcdef.afslyr")
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatalf("MkdirAll layer dir: %v", err)
	}
	if err := os.WriteFile(layerPath, []byte("layer"), 0o644); err != nil {
		t.Fatalf("WriteFile layer: %v", err)
	}

	got, err := scanCachedImageKeys(tmp, layerformat.FormatV1)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 1 || got[0] != "nginx|latest|linux|amd64||v1" {
		t.Fatalf("got=%v, want [nginx|latest|linux|amd64||v1]", got)
	}
}

func TestScanCachedImageKeys_LayersMissing(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "metadata", "a.json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := `{"image":"nginx","tag":"latest","platform_os":"linux","platform_arch":"amd64","platform_variant":"","layers":[{"Digest":"sha256:abcdef"}]}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := scanCachedImageKeys(tmp, layerformat.FormatV1)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got=%v, want []", got)
	}
}

func TestScanCachedLayerStats_V2DoesNotSeeV1(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	// Place only a V1 .afslyr file
	layerPath := filepath.Join(tmp, "layers", "sha256", "abcdef.afslyr")
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(layerPath, []byte("v1data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := scanCachedLayerStats(tmp, layerformat.FormatV2)
	if err != nil {
		t.Fatalf("scanCachedLayerStats: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("V2 scan should not see V1 file, got=%v", got)
	}
}

func TestScanCachedLayerStats_V1DoesNotSeeV2(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	// Place only a V2 .v2.afslyr file
	layerPath := filepath.Join(tmp, "layers", "sha256", "abcdef.v2.afslyr")
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(layerPath, []byte("v2data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := scanCachedLayerStats(tmp, layerformat.FormatV1)
	if err != nil {
		t.Fatalf("scanCachedLayerStats: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("V1 scan should not see V2 file, got=%v", got)
	}
}

func TestScanCachedImageKeys_V2DoesNotSeeV1Layers(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "metadata", "a.json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := `{"image":"nginx","tag":"latest","platform_os":"linux","platform_arch":"amd64","platform_variant":"","layers":[{"Digest":"sha256:abcdef"}]}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Only place a V1 layer file
	layerPath := filepath.Join(tmp, "layers", "sha256", "abcdef.afslyr")
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatalf("MkdirAll layer dir: %v", err)
	}
	if err := os.WriteFile(layerPath, []byte("v1layer"), 0o644); err != nil {
		t.Fatalf("WriteFile layer: %v", err)
	}

	got, err := scanCachedImageKeys(tmp, layerformat.FormatV2)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("V2 image scan should not see V1 layers as ready, got=%v", got)
	}
}

func TestScanCachedImageKeys_V1DoesNotSeeV2Layers(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "metadata", "a.json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := `{"image":"nginx","tag":"latest","platform_os":"linux","platform_arch":"amd64","platform_variant":"","layers":[{"Digest":"sha256:abcdef"}]}`
	if err := os.WriteFile(metaPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Only place a V2 layer file
	layerPath := filepath.Join(tmp, "layers", "sha256", "abcdef.v2.afslyr")
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatalf("MkdirAll layer dir: %v", err)
	}
	if err := os.WriteFile(layerPath, []byte("v2layer"), 0o644); err != nil {
		t.Fatalf("WriteFile layer: %v", err)
	}

	got, err := scanCachedImageKeys(tmp, layerformat.FormatV1)
	if err != nil {
		t.Fatalf("scanCachedImageKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("V1 image scan should not see V2 layers as ready, got=%v", got)
	}
}

func TestImageKeyIncludesFormatVersion(t *testing.T) {
	t.Parallel()

	v1Key := imageKey("nginx", "latest", "linux", "amd64", "", layerformat.FormatV1)
	v2Key := imageKey("nginx", "latest", "linux", "amd64", "", layerformat.FormatV2)
	if v1Key == v2Key {
		t.Fatalf("V1 and V2 image keys should differ, both=%q", v1Key)
	}
	if !strings.HasSuffix(v1Key, "|v1") {
		t.Fatalf("V1 key should end with |v1, got=%q", v1Key)
	}
	if !strings.HasSuffix(v2Key, "|v2") {
		t.Fatalf("V2 key should end with |v2, got=%q", v2Key)
	}
}
