package afsmount

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildLinuxOverlayOptionsOrdering(t *testing.T) {
	t.Parallel()

	got, err := buildLinuxOverlayOptions(
		[]string{"/layers/l0", "/layers/l1", "/layers/l2"},
		"/tmp/upper",
		"/tmp/work",
		"/tmp/extra",
	)
	if err != nil {
		t.Fatalf("buildLinuxOverlayOptions() error: %v", err)
	}

	want := []string{
		"lowerdir=/tmp/extra:/layers/l2:/layers/l1:/layers/l0",
		"upperdir=/tmp/upper",
		"workdir=/tmp/work",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildLinuxOverlayOptions() = %v, want %v", got, want)
	}
}

func TestBuildLinuxOverlayOptionsRequiresWorkdirForUpper(t *testing.T) {
	t.Parallel()

	_, err := buildLinuxOverlayOptions([]string{"/layers/l0"}, "/tmp/upper", "", "")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "writable work dir is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeConfigDefaultsReadAheadAndDiscovery(t *testing.T) {
	t.Parallel()

	cfg, err := normalizeConfig(Config{
		Mountpoint: "/tmp/mountpoint",
		Image:      "alpine",
	})
	if err != nil {
		t.Fatalf("normalizeConfig() error: %v", err)
	}
	if cfg.fuseMaxReadAheadBytes != DefaultFUSEMaxReadAhead {
		t.Fatalf("fuseMaxReadAheadBytes=%d, want %d", cfg.fuseMaxReadAheadBytes, DefaultFUSEMaxReadAhead)
	}
	if cfg.discoveryAddr != "127.0.0.1:60051" {
		t.Fatalf("discoveryAddr=%q, want default", cfg.discoveryAddr)
	}
}
