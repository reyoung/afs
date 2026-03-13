package layerreader

import (
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type ObserveConfig struct {
	Name        string
	LogEvery    time.Duration
	LogMinCalls int64
	LogMinBytes int64
}

type observedReaderAt struct {
	name string
	base io.ReaderAt

	logEvery    time.Duration
	logMinCalls int64
	logMinBytes int64

	readCalls    atomic.Int64
	reqBytes     atomic.Int64
	gotBytes     atomic.Int64
	shortReads   atomic.Int64
	eofReads     atomic.Int64
	overlapCalls atomic.Int64
	inflight     atomic.Int64
	maxInflight  atomic.Int64
	lastLogAtNs  atomic.Int64
}

func NewObservedReaderAt(base io.ReaderAt, cfg ObserveConfig) io.ReaderAt {
	if base == nil {
		return nil
	}
	logEvery := cfg.LogEvery
	if logEvery <= 0 {
		logEvery = envDurationOrDefault("AFS_DISCOVERY_READER_LOG_INTERVAL", 0)
	}
	if logEvery <= 0 {
		return base
	}
	logMinCalls := cfg.LogMinCalls
	if logMinCalls <= 0 {
		logMinCalls = envInt64OrDefault("AFS_DISCOVERY_READER_LOG_MIN_CALLS", 128)
	}
	logMinBytes := cfg.LogMinBytes
	if logMinBytes <= 0 {
		logMinBytes = envInt64OrDefault("AFS_DISCOVERY_READER_LOG_MIN_BYTES", 32<<20)
	}
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = "reader"
	}
	return &observedReaderAt{
		name:        name,
		base:        base,
		logEvery:    logEvery,
		logMinCalls: logMinCalls,
		logMinBytes: logMinBytes,
	}
}

func (r *observedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	currentInflight := r.inflight.Add(1)
	updateObservedReaderMax(&r.maxInflight, currentInflight)
	if currentInflight > 1 {
		r.overlapCalls.Add(1)
	}
	defer r.inflight.Add(-1)

	n, err := r.base.ReadAt(p, off)
	r.recordReadStats(int64(len(p)), int64(n), off, err)
	return n, err
}

func (r *observedReaderAt) recordReadStats(reqBytes, gotBytes, off int64, err error) {
	r.readCalls.Add(1)
	r.reqBytes.Add(reqBytes)
	r.gotBytes.Add(gotBytes)
	if gotBytes < reqBytes {
		r.shortReads.Add(1)
	}
	if errors.Is(err, io.EOF) {
		r.eofReads.Add(1)
	}
	r.maybeLogReadStats(off)
}

func (r *observedReaderAt) maybeLogReadStats(off int64) {
	calls := r.readCalls.Load()
	if calls < r.logMinCalls {
		return
	}
	gotBytes := r.gotBytes.Load()
	if gotBytes < r.logMinBytes {
		return
	}
	now := time.Now()
	nowNs := now.UnixNano()
	lastNs := r.lastLogAtNs.Load()
	if lastNs != 0 && nowNs-lastNs < r.logEvery.Nanoseconds() {
		return
	}
	if !r.lastLogAtNs.CompareAndSwap(lastNs, nowNs) {
		return
	}

	reqBytes := r.reqBytes.Load()
	shortReads := r.shortReads.Load()
	eofReads := r.eofReads.Load()
	overlapCalls := r.overlapCalls.Load()
	maxInflight := r.maxInflight.Load()
	avgReq := int64(0)
	avgGot := int64(0)
	if calls > 0 {
		avgReq = reqBytes / calls
		avgGot = gotBytes / calls
	}
	overlapPct := 0.0
	if calls > 0 {
		overlapPct = float64(overlapCalls) / float64(calls) * 100
	}
	log.Printf("layer read stats: name=%s calls=%d req_bytes=%d got_bytes=%d avg_req=%d avg_got=%d short_reads=%d eof_reads=%d overlap_calls=%d overlap_pct=%.2f max_inflight=%d inflight=%d last_off=%d",
		r.name, calls, reqBytes, gotBytes, avgReq, avgGot, shortReads, eofReads, overlapCalls, overlapPct, maxInflight, r.inflight.Load(), off)
}

func envDurationOrDefault(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return v
}

func envInt64OrDefault(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

func updateObservedReaderMax(dst *atomic.Int64, current int64) {
	for {
		old := dst.Load()
		if current <= old {
			return
		}
		if dst.CompareAndSwap(old, current) {
			return
		}
	}
}
