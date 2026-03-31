package filecache

import (
	"container/list"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

type entry struct {
	digest      string
	path        string
	size        int64
	elem        *list.Element
	ready       chan struct{}
	err         error
	fd          *os.File
	pinCount    int
	invalidated bool
}

type Store struct {
	dir          string
	maxBytes     int64
	maxFileBytes int64

	mu      sync.Mutex
	lru     *list.List
	entries map[string]*entry
	used    int64

	hitCount  atomic.Int64
	missCount atomic.Int64

	bypassCount      atomic.Int64
	bypassBytes      atomic.Int64
	cacheReadBytes   atomic.Int64
	directReadBytes  atomic.Int64
	materializeCount atomic.Int64
	materializeBytes atomic.Int64
	evictCount       atomic.Int64
	evictBytes       atomic.Int64
	errorCount       atomic.Int64
}

func NewStore(cacheDir string, maxBytes int64, maxFileBytes int64) (*Store, error) {
	if strings.TrimSpace(cacheDir) == "" {
		return nil, fmt.Errorf("cache dir is required")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("max bytes must be > 0")
	}
	if maxFileBytes <= 0 {
		return nil, fmt.Errorf("max file bytes must be > 0")
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	return &Store{
		dir:          cacheDir,
		maxBytes:     maxBytes,
		maxFileBytes: maxFileBytes,
		lru:          list.New(),
		entries:      make(map[string]*entry),
	}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.fd != nil {
			_ = e.fd.Close()
			e.fd = nil
		}
	}
	return nil
}
func (s *Store) HitCount() int64  { return s.hitCount.Load() }
func (s *Store) MissCount() int64 { return s.missCount.Load() }

type Stats struct {
	Hits             int64
	Misses           int64
	Bypasses         int64
	BypassBytes      int64
	CacheReadBytes   int64
	DirectReadBytes  int64
	Materializations int64
	MaterializeBytes int64
	Evictions        int64
	EvictBytes       int64
	Errors           int64
	Entries          int
	UsedBytes        int64
	MaxBytes         int64
	MaxFileBytes     int64
}

func (s *Store) Snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Hits:             s.hitCount.Load(),
		Misses:           s.missCount.Load(),
		Bypasses:         s.bypassCount.Load(),
		BypassBytes:      s.bypassBytes.Load(),
		CacheReadBytes:   s.cacheReadBytes.Load(),
		DirectReadBytes:  s.directReadBytes.Load(),
		Materializations: s.materializeCount.Load(),
		MaterializeBytes: s.materializeBytes.Load(),
		Evictions:        s.evictCount.Load(),
		EvictBytes:       s.evictBytes.Load(),
		Errors:           s.errorCount.Load(),
		Entries:          len(s.entries),
		UsedBytes:        s.used,
		MaxBytes:         s.maxBytes,
		MaxFileBytes:     s.maxFileBytes,
	}
}

func (s *Store) Log(prefix string) {
	st := s.Snapshot()
	log.Printf("%s ELF cache stats: hits=%d misses=%d bypasses=%d bypass_bytes=%s cache_read_bytes=%s direct_read_bytes=%s materialized=%d/%s evicted=%d/%s errors=%d entries=%d used=%s limit=%s max_file=%s",
		prefix,
		st.Hits,
		st.Misses,
		st.Bypasses,
		fmtBytes(st.BypassBytes),
		fmtBytes(st.CacheReadBytes),
		fmtBytes(st.DirectReadBytes),
		st.Materializations,
		fmtBytes(st.MaterializeBytes),
		st.Evictions,
		fmtBytes(st.EvictBytes),
		st.Errors,
		st.Entries,
		fmtBytes(st.UsedBytes),
		fmtBytes(st.MaxBytes),
		fmtBytes(st.MaxFileBytes),
	)
}

func (s *Store) ReadThrough(underlying io.ReaderAt, digest string, fileSize int64, dest []byte, off int64) (int, error) {
	if off >= fileSize {
		return 0, io.EOF
	}
	if digest == "" || fileSize <= 0 || fileSize > s.maxFileBytes {
		s.bypassCount.Add(1)
		n, err := readDirect(underlying, fileSize, dest, off)
		if n > 0 {
			s.bypassBytes.Add(int64(n))
			s.directReadBytes.Add(int64(n))
		}
		return n, err
	}

	e, leader := s.acquire(digest)
	if leader {
		s.materialize(e, underlying, fileSize)
	}
	<-e.ready
	if e.err != nil {
		n, err := readDirect(underlying, fileSize, dest, off)
		if n > 0 {
			s.directReadBytes.Add(int64(n))
		}
		return n, err
	}
	if !leader {
		s.hitCount.Add(1)
	}
	if !s.touchAndPin(e) {
		n, err := readDirect(underlying, fileSize, dest, off)
		if n > 0 {
			s.directReadBytes.Add(int64(n))
		}
		return n, err
	}
	n, err := readFromEntry(e, fileSize, dest, off)
	s.unpinEntry(e)
	if n > 0 {
		s.cacheReadBytes.Add(int64(n))
	}
	if err != nil && err != io.EOF {
		s.errorCount.Add(1)
		log.Printf("ELF cache read failed, falling back to direct read: digest=%s path=%s err=%v", e.digest, e.path, err)
		s.invalidateEntry(e)
		n, err = readDirect(underlying, fileSize, dest, off)
		if n > 0 {
			s.directReadBytes.Add(int64(n))
		}
		return n, err
	}
	return n, err
}

func (s *Store) acquire(digest string) (*entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[digest]; ok {
		return e, false
	}
	e := &entry{digest: digest, ready: make(chan struct{})}
	s.entries[digest] = e
	s.missCount.Add(1)
	return e, true
}

func (s *Store) materialize(e *entry, underlying io.ReaderAt, fileSize int64) {
	defer close(e.ready)

	tmp, err := os.CreateTemp(s.dir, ".tmp-elf-*")
	if err != nil {
		s.failEntry(e, fmt.Errorf("create temp file: %w", err))
		return
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(tmp, io.NewSectionReader(underlying, 0, fileSize)); err != nil {
		s.failEntry(e, fmt.Errorf("copy ELF payload: %w", err))
		return
	}
	if err := tmp.Sync(); err != nil {
		s.failEntry(e, fmt.Errorf("sync ELF cache file: %w", err))
		return
	}

	finalPath := filepath.Join(s.dir, sanitizeDigest(e.digest))
	if err := os.Rename(tmpPath, finalPath); err != nil {
		s.failEntry(e, fmt.Errorf("rename ELF cache file: %w", err))
		return
	}
	ok = true
	fd, err := os.Open(finalPath)
	if err != nil {
		s.failEntry(e, fmt.Errorf("open ELF cache file: %w", err))
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	e.path = finalPath
	e.size = fileSize
	e.fd = fd
	e.elem = s.lru.PushFront(e.digest)
	s.used += fileSize
	s.materializeCount.Add(1)
	s.materializeBytes.Add(fileSize)
	s.evictLocked()
}

func (s *Store) failEntry(e *entry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errorCount.Add(1)
	e.err = err
	if e.fd != nil {
		_ = e.fd.Close()
		e.fd = nil
	}
	delete(s.entries, e.digest)
}

func (s *Store) touch(e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.elem != nil {
		s.lru.MoveToFront(e.elem)
	}
}

// touchAndPin moves e to the LRU front and increments its pin count atomically.
// Returns false if the entry is no longer valid (invalidated, errored, or no open fd).
func (s *Store) touchAndPin(e *entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.elem != nil {
		s.lru.MoveToFront(e.elem)
	}
	if e.invalidated || e.err != nil || e.fd == nil {
		return false
	}
	e.pinCount++
	return true
}

func (s *Store) evictLocked() {
	for s.used > s.maxBytes {
		var victim *entry
		for cur := s.lru.Back(); cur != nil; cur = cur.Prev() {
			digest := cur.Value.(string)
			e := s.entries[digest]
			if e == nil {
				s.lru.Remove(cur)
				continue
			}
			if e.pinCount > 0 {
				continue
			}
			victim = e
			break
		}
		if victim == nil {
			return
		}
		s.evictCount.Add(1)
		s.evictBytes.Add(victim.size)
		s.removeEntryLocked(victim)
	}
}

func (s *Store) pinEntry(e *entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.invalidated || e.err != nil || e.fd == nil {
		return false
	}
	e.pinCount++
	return true
}

func (s *Store) unpinEntry(e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.pinCount > 0 {
		e.pinCount--
	}
	if e.pinCount == 0 && e.invalidated {
		s.removeEntryLocked(e)
	}
}

func (s *Store) invalidateEntry(e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.entries[e.digest]
	if !ok || current != e {
		return
	}
	e.invalidated = true
	if e.pinCount == 0 {
		s.removeEntryLocked(e)
	}
}

func (s *Store) removeEntryLocked(e *entry) {
	if e == nil {
		return
	}
	if current, ok := s.entries[e.digest]; ok && current == e {
		delete(s.entries, e.digest)
	}
	if e.elem != nil {
		s.lru.Remove(e.elem)
		e.elem = nil
	}
	if e.size > 0 {
		s.used -= e.size
		if s.used < 0 {
			s.used = 0
		}
	}
	if e.fd != nil {
		_ = e.fd.Close()
		e.fd = nil
	}
	e.invalidated = true
	if e.path != "" {
		_ = os.Remove(e.path)
	}
}

func fmtBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%dB", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	}
	if b < 1024*1024*1024 {
		return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
	}
	return fmt.Sprintf("%.1fGB", float64(b)/(1024*1024*1024))
}

func sanitizeDigest(digest string) string {
	name := strings.ReplaceAll(digest, ":", "_")
	name = strings.ReplaceAll(name, "/", "_")
	return name
}

func readDirect(underlying io.ReaderAt, fileSize int64, dest []byte, off int64) (int, error) {
	remain := fileSize - off
	if remain > int64(len(dest)) {
		remain = int64(len(dest))
	}
	if remain <= 0 {
		return 0, io.EOF
	}
	n, err := underlying.ReadAt(dest[:remain], off)
	if err == io.EOF && n > 0 {
		return n, nil
	}
	return n, err
}

func readFromEntry(e *entry, fileSize int64, dest []byte, off int64) (int, error) {
	if e == nil || e.fd == nil {
		return 0, fmt.Errorf("cache file is not open")
	}
	remain := fileSize - off
	if remain > int64(len(dest)) {
		remain = int64(len(dest))
	}
	if remain <= 0 {
		return 0, io.EOF
	}
	n, err := e.fd.ReadAt(dest[:remain], off)
	if err == io.EOF && n > 0 {
		return n, nil
	}
	return n, err
}
