package afsmount

import (
	"reflect"
	"strings"
	"testing"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerreader"
)

func TestSharedCatalogLayerRefcountDelaysRemoteCloseUntilLastRelease(t *testing.T) {
	mgr := &sharedCatalogManager{
		images: make(map[string]*sharedCatalogImage),
		layers: make(map[string]*sharedCatalogLayer),
	}

	remoteA := &layerreader.DiscoveryBackedReaderAt{}
	layerA := LayerInfo{Digest: "sha256:shared", Remote: remoteA, Reader: &layerformat.Reader{}}
	adopted, err := mgr.acquireLayerLocked(layerA)
	if err != nil {
		t.Fatalf("acquire first layer: %v", err)
	}
	if adopted.Remote != remoteA {
		t.Fatalf("expected first acquire to keep original remote")
	}

	remoteB := &layerreader.DiscoveryBackedReaderAt{}
	layerB := LayerInfo{Digest: "sha256:shared", Remote: remoteB, Reader: &layerformat.Reader{}}
	reused, err := mgr.acquireLayerLocked(layerB)
	if err != nil {
		t.Fatalf("acquire second layer: %v", err)
	}
	if reused.Remote != remoteA {
		t.Fatalf("expected second acquire to reuse first remote")
	}
	if got := mgr.layers["sha256:shared"].refs; got != 2 {
		t.Fatalf("expected refcount 2, got %d", got)
	}
	if _, err := remoteB.ReadAt(make([]byte, 1), 0); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected reused remote to be closed, got %v", err)
	}

	mgr.releaseLayersLocked([]string{"sha256:shared"})
	if got := mgr.layers["sha256:shared"].refs; got != 1 {
		t.Fatalf("expected refcount 1 after first release, got %d", got)
	}
	if readerClosed(remoteA) {
		t.Fatalf("expected shared remote to remain open after first release")
	}

	mgr.releaseLayersLocked([]string{"sha256:shared"})
	if _, ok := mgr.layers["sha256:shared"]; ok {
		t.Fatalf("expected shared layer entry to be removed after last release")
	}
	if _, err := remoteA.ReadAt(make([]byte, 1), 0); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected shared remote to be closed after last release, got %v", err)
	}
}

func readerClosed(r *layerreader.DiscoveryBackedReaderAt) bool {
	return reflect.ValueOf(r).Elem().FieldByName("closed").Bool()
}
