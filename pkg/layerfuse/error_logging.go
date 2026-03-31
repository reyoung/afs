package layerfuse

import (
	"log"

	"github.com/reyoung/afs/pkg/layerformat"
)

func logReadFailure(source string, entry layerformat.Entry, section layerformat.FileSection, off int64, reqBytes int, cacheMode string, err error) {
	log.Printf("%s: path=%s digest=%s content_kind=%v cache=%s off=%d req=%d file_size=%d section_offset=%d err=%v",
		source,
		entry.Path,
		entry.Digest,
		entry.ContentKind,
		cacheMode,
		off,
		reqBytes,
		section.Size,
		section.Offset,
		err,
	)
}
