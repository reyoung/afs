package layerfuse

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// FuseStats tracks per-operation call counts and cumulative durations.
type FuseStats struct {
	LookupCount    atomic.Int64
	LookupNanos    atomic.Int64
	GetattrCount   atomic.Int64
	GetattrNanos   atomic.Int64
	OpenCount      atomic.Int64
	OpenNanos      atomic.Int64
	ReadCount      atomic.Int64
	ReadNanos      atomic.Int64
	ReadBytes      atomic.Int64
	ReaddirCount   atomic.Int64
	ReaddirNanos   atomic.Int64
	ReadlinkCount  atomic.Int64
	ReadlinkNanos  atomic.Int64
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
