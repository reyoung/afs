package pagecache

import "sync"

// PinnedReadFD describes a chunk-backed byte range that must stay pinned until
// the caller finishes sending it back to the kernel.
type PinnedReadFD struct {
	FD     uintptr
	Offset int64
	Size   int

	releaseOnce sync.Once
	releaseFn   func()
}

// Release drops the pin held for this read result. It is safe to call more
// than once.
func (r *PinnedReadFD) Release() {
	if r == nil {
		return
	}
	r.releaseOnce.Do(func() {
		if r.releaseFn != nil {
			r.releaseFn()
		}
	})
}
