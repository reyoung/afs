package layerfuse

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FuseStats tracks per-operation call counts and cumulative durations.
type FuseStats struct {
	LookupCount   atomic.Int64
	LookupNanos   atomic.Int64
	GetattrCount  atomic.Int64
	GetattrNanos  atomic.Int64
	OpenCount     atomic.Int64
	OpenNanos     atomic.Int64
	ReadCount     atomic.Int64
	ReadNanos     atomic.Int64
	ReadBytes     atomic.Int64
	ReaddirCount  atomic.Int64
	ReaddirNanos  atomic.Int64
	ReadlinkCount atomic.Int64
	ReadlinkNanos atomic.Int64

	hotMu            sync.Mutex
	lookupHotPaths   map[string]int64
	openHotPaths     map[string]int64
	readHotPaths     map[string]int64
	readHotPathBytes map[string]int64
	readlinkHotPaths map[string]int64
}

// Log prints a summary of FUSE operation statistics.
func (s *FuseStats) Log(prefix string) {
	log.Printf("%s FUSE stats: lookup=%d/%s getattr=%d/%s open=%d/%s read=%d/%s/%s readdir=%d/%s readlink=%d/%s",
		prefix,
		s.LookupCount.Load(), fmtDur(s.LookupNanos.Load()),
		s.GetattrCount.Load(), fmtDur(s.GetattrNanos.Load()),
		s.OpenCount.Load(), fmtDur(s.OpenNanos.Load()),
		s.ReadCount.Load(), fmtDur(s.ReadNanos.Load()), fmtBytes(s.ReadBytes.Load()),
		s.ReaddirCount.Load(), fmtDur(s.ReaddirNanos.Load()),
		s.ReadlinkCount.Load(), fmtDur(s.ReadlinkNanos.Load()),
	)
	s.logHotPaths(prefix)
}

func (s *FuseStats) RecordLookupPath(path string) {
	s.recordHotPath(&s.lookupHotPaths, path)
}

func (s *FuseStats) RecordOpenPath(path string) {
	s.recordHotPath(&s.openHotPaths, path)
}

func (s *FuseStats) RecordReadPath(path string, bytes int64) {
	if hotPathLimit() == 0 || path == "" {
		return
	}
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	if s.readHotPaths == nil {
		s.readHotPaths = make(map[string]int64)
	}
	if s.readHotPathBytes == nil {
		s.readHotPathBytes = make(map[string]int64)
	}
	s.readHotPaths[path]++
	s.readHotPathBytes[path] += bytes
}

func (s *FuseStats) RecordReadlinkPath(path string) {
	s.recordHotPath(&s.readlinkHotPaths, path)
}

func (s *FuseStats) recordHotPath(dst *map[string]int64, path string) {
	if hotPathLimit() == 0 || path == "" {
		return
	}
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	if *dst == nil {
		*dst = make(map[string]int64)
	}
	(*dst)[path]++
}

func (s *FuseStats) logHotPaths(prefix string) {
	limit := hotPathLimit()
	if limit == 0 {
		return
	}

	s.hotMu.Lock()
	defer s.hotMu.Unlock()

	s.logOneHotPathSet(prefix, "lookup", s.lookupHotPaths, limit)
	s.logOneHotPathSet(prefix, "open", s.openHotPaths, limit)
	s.logOneHotPathSet(prefix, "read", s.readHotPaths, limit)
	s.logOneHotPathBytes(prefix, "read", s.readHotPathBytes, limit)
	s.logOneHotPathSet(prefix, "readlink", s.readlinkHotPaths, limit)
}

func (s *FuseStats) logOneHotPathSet(prefix, op string, paths map[string]int64, limit int) {
	if len(paths) == 0 {
		return
	}
	type hotPath struct {
		path  string
		count int64
	}
	items := make([]hotPath, 0, len(paths))
	for path, count := range paths {
		items = append(items, hotPath{path: path, count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].path < items[j].path
		}
		return items[i].count > items[j].count
	})
	if len(items) > limit {
		items = items[:limit]
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s:%d", item.path, item.count))
	}
	log.Printf("%s FUSE hot %s paths: %s", prefix, op, strings.Join(parts, ", "))
}

func (s *FuseStats) logOneHotPathBytes(prefix, op string, paths map[string]int64, limit int) {
	if len(paths) == 0 {
		return
	}
	type hotPath struct {
		path  string
		bytes int64
	}
	items := make([]hotPath, 0, len(paths))
	for path, bytes := range paths {
		items = append(items, hotPath{path: path, bytes: bytes})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].bytes == items[j].bytes {
			return items[i].path < items[j].path
		}
		return items[i].bytes > items[j].bytes
	})
	if len(items) > limit {
		items = items[:limit]
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s:%s", item.path, fmtBytes(item.bytes)))
	}
	log.Printf("%s FUSE hot %s bytes: %s", prefix, op, strings.Join(parts, ", "))
}

func hotPathLimit() int {
	v := os.Getenv("AFS_FUSE_HOT_PATH_LIMIT")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func fmtDur(nanos int64) string {
	d := time.Duration(nanos)
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Nanoseconds())/1e6)
}

func fmtBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%dB", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	}
	return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
}
