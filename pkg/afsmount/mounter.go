package afsmount

import (
	"context"

	"github.com/reyoung/afs/pkg/filecache"
	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerfuse"
	"github.com/reyoung/afs/pkg/layerreader"
	"github.com/reyoung/afs/pkg/pagecache"
)

// MountResult holds a mounted filesystem ready for runc.
type MountResult struct {
	// LayerDirs is the list of read-only layer directories used as lowerdirs.
	LayerDirs []string
	// Cleanup unmounts everything and releases resources. Must be called exactly once.
	Cleanup func()
	// Stats holds FUSE statistics for the mounted lower filesystem.
	Stats []*layerfuse.FuseStats
}

// LayerInfo describes a single OCI layer for mounting.
type LayerInfo struct {
	Digest string
	Reader *layerformat.Reader
	Remote *layerreader.DiscoveryBackedReaderAt
}

// MountConfig holds all configuration needed to mount layers.
type MountConfig struct {
	Layers     []LayerInfo
	WorkDir    string // working directory for mount temps
	Debug      bool
	ReadAhead  int64
	PageCache  *pagecache.Store
	ELFCache   *filecache.Store
	TOCCache   *layerformat.TOCCache
	HoldReaper func() func()

	ExtraDir    string
	WritableDir string
}

// Mounter prepares a read-only rootfs from a set of OCI layers.
type Mounter interface {
	Mount(ctx context.Context, cfg MountConfig) (*MountResult, error)
}
