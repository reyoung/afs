package spillcache

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	defaultSampleSize = 5
	indexDBFileName   = "index.db"
)

var metadataBucket = []byte("entries")

type cacheKey struct {
	Digest   string `json:"digest"`
	FilePath string `json:"file_path"`
}

func (k cacheKey) normalize() (cacheKey, error) {
	d := strings.TrimSpace(k.Digest)
	p := strings.TrimSpace(k.FilePath)
	if d == "" || p == "" {
		return cacheKey{}, errors.New("digest and file_path are required")
	}
	return cacheKey{Digest: d, FilePath: p}, nil
}

func (k cacheKey) keyString() string {
	return k.Digest + "\n" + k.FilePath
}

func (k cacheKey) hash() string {
	sum := sha256.Sum256([]byte(k.keyString()))
	return hex.EncodeToString(sum[:])
}

type entry struct {
	Digest         string `json:"digest"`
	FilePath       string `json:"file_path"`
	Hash           string `json:"hash"`
	DataFile       string `json:"data_file"`
	Size           int64  `json:"size"`
	LastAccessUnix int64  `json:"last_access_unix"`
}

type lease struct {
	Token    string
	Key      cacheKey
	Hash     string
	TempPath string
}

type cacheStore struct {
	mu         sync.Mutex
	cacheDir   string
	dataDir    string
	tmpDir     string
	dbPath     string
	maxBytes   int64
	totalBytes int64
	entries    map[string]*entry
	leases     map[string]*lease
	rnd        *rand.Rand
	db         *bolt.DB
}

func newCacheStore(cacheDir string, maxBytes int64) (*cacheStore, error) {
	cacheDir = strings.TrimSpace(cacheDir)
	if cacheDir == "" {
		return nil, fmt.Errorf("cache directory is required")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("max bytes must be > 0")
	}
	cs := &cacheStore{
		cacheDir: cacheDir,
		dataDir:  filepath.Join(cacheDir, "data"),
		tmpDir:   filepath.Join(cacheDir, "tmp"),
		dbPath:   filepath.Join(cacheDir, indexDBFileName),
		maxBytes: maxBytes,
		entries:  make(map[string]*entry),
		leases:   make(map[string]*lease),
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if err := os.MkdirAll(cs.dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	if err := os.MkdirAll(cs.tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("create tmp dir: %w", err)
	}
	db, err := bolt.Open(cs.dbPath, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open index db: %w", err)
	}
	cs.db = db
	if err := cs.initDB(); err != nil {
		_ = cs.db.Close()
		return nil, err
	}
	if err := cs.loadEntries(); err != nil {
		_ = cs.db.Close()
		return nil, err
	}
	return cs, nil
}

func (c *cacheStore) acquire(key cacheKey) (hitPath string, hitSize int64, outLease *lease, err error) {
	nk, err := key.normalize()
	if err != nil {
		return "", 0, nil, err
	}
	keyStr := nk.keyString()
	hash := nk.hash()

	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.entries[keyStr]; ok {
		e.LastAccessUnix = time.Now().Unix()
		return e.DataFile, e.Size, nil, nil
	}

	token := randomToken(c.rnd)
	tmpPath := filepath.Join(c.tmpDir, token+".tmp")
	f, createErr := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if createErr != nil {
		return "", 0, nil, createErr
	}
	_ = f.Close()

	l := &lease{Token: token, Key: nk, Hash: hash, TempPath: tmpPath}
	c.leases[token] = l
	return "", 0, l, nil
}

func (c *cacheStore) commit(token string) (cachePath string, size int64, err error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", 0, fmt.Errorf("lease token is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	l, ok := c.leases[token]
	if !ok {
		return "", 0, fmt.Errorf("lease token not found")
	}
	defer delete(c.leases, token)

	st, err := os.Stat(l.TempPath)
	if err != nil {
		return "", 0, fmt.Errorf("stat lease temp file: %w", err)
	}
	size = st.Size()
	keyStr := l.Key.keyString()
	if existing, ok := c.entries[keyStr]; ok {
		existing.LastAccessUnix = time.Now().Unix()
		_ = os.Remove(l.TempPath)
		if err := c.persistEntryLocked(keyStr, existing); err != nil {
			return "", 0, err
		}
		return existing.DataFile, existing.Size, nil
	}

	finalPath := filepath.Join(c.dataDir, l.Hash+".spill")
	if err := os.Rename(l.TempPath, finalPath); err != nil {
		return "", 0, fmt.Errorf("commit cache file: %w", err)
	}
	c.entries[keyStr] = &entry{
		Digest:         l.Key.Digest,
		FilePath:       l.Key.FilePath,
		Hash:           l.Hash,
		DataFile:       finalPath,
		Size:           size,
		LastAccessUnix: time.Now().Unix(),
	}
	c.totalBytes += size
	if err := c.evictIfNeededLocked(keyStr); err != nil {
		return "", 0, err
	}
	if err := c.persistEntryLocked(keyStr, c.entries[keyStr]); err != nil {
		return "", 0, err
	}
	if e, ok := c.entries[keyStr]; ok {
		return e.DataFile, e.Size, nil
	}
	return "", 0, fmt.Errorf("entry evicted during commit")
}

func (c *cacheStore) abort(token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.leases[token]
	if !ok {
		return nil
	}
	delete(c.leases, token)
	_ = os.Remove(l.TempPath)
	return nil
}

func (c *cacheStore) evictIfNeededLocked(protectedKey string) error {
	for c.totalBytes > c.maxBytes {
		victimKey := c.pickApproxLRUVictimLocked(defaultSampleSize, protectedKey)
		if victimKey == "" {
			victimKey = c.pickDeterministicVictimLocked(protectedKey)
			if victimKey == "" {
				break
			}
		}
		e := c.entries[victimKey]
		if e == nil {
			delete(c.entries, victimKey)
			c.totalBytes = c.recomputeTotalBytesLocked()
			continue
		}
		if err := os.Remove(e.DataFile); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove cache file %s: %w", e.DataFile, err)
		}
		delete(c.entries, victimKey)
		c.totalBytes -= e.Size
		if c.totalBytes < 0 {
			c.totalBytes = 0
		}
		if err := c.deleteEntryLocked(victimKey); err != nil {
			return err
		}
	}
	return nil
}

func (c *cacheStore) pickDeterministicVictimLocked(protectedKey string) string {
	if len(c.entries) == 0 {
		return ""
	}
	allowProtected := len(c.entries) <= 1
	var victim string
	var victimAt int64
	hasVictim := false
	for k, e := range c.entries {
		if !allowProtected && protectedKey != "" && k == protectedKey {
			continue
		}
		candidateAt := int64(0)
		if e != nil {
			candidateAt = e.LastAccessUnix
		}
		if !hasVictim || candidateAt < victimAt || (candidateAt == victimAt && k < victim) {
			victim = k
			victimAt = candidateAt
			hasVictim = true
		}
	}
	return victim
}

func (c *cacheStore) pickApproxLRUVictimLocked(sampleSize int, protectedKey string) string {
	if len(c.entries) == 0 {
		return ""
	}
	keys := make([]string, 0, len(c.entries))
	for k := range c.entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if sampleSize > len(keys) {
		sampleSize = len(keys)
	}
	if sampleSize <= 0 {
		sampleSize = 1
	}

	var victim string
	victimAt := int64(0)
	for i := 0; i < sampleSize; i++ {
		candidate := keys[c.rnd.Intn(len(keys))]
		if protectedKey != "" && candidate == protectedKey && len(c.entries) > 1 {
			continue
		}
		e := c.entries[candidate]
		if e == nil {
			continue
		}
		if victim == "" || e.LastAccessUnix < victimAt {
			victim = candidate
			victimAt = e.LastAccessUnix
		}
	}
	return victim
}

func (c *cacheStore) recomputeTotalBytesLocked() int64 {
	var total int64
	for _, e := range c.entries {
		total += e.Size
	}
	return total
}

func randomToken(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	buf := make([]byte, 24)
	for i := range buf {
		buf[i] = letters[r.Intn(len(letters))]
	}
	return string(buf)
}

func (c *cacheStore) initDB() error {
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return fmt.Errorf("create metadata bucket: %w", err)
		}
		return nil
	})
}

func (c *cacheStore) loadEntries() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			e, err := decodeEntry(v)
			if err != nil {
				return fmt.Errorf("decode entry %q: %w", string(k), err)
			}
			key, normErr := (cacheKey{Digest: e.Digest, FilePath: e.FilePath}).normalize()
			if normErr != nil {
				return nil
			}
			if strings.TrimSpace(e.DataFile) == "" {
				return nil
			}
			st, statErr := os.Stat(e.DataFile)
			if statErr != nil {
				return nil
			}
			e.Size = st.Size()
			if e.LastAccessUnix <= 0 {
				e.LastAccessUnix = st.ModTime().Unix()
			}
			c.entries[key.keyString()] = e
			return nil
		})
	}); err != nil {
		return fmt.Errorf("load index db: %w", err)
	}
	c.totalBytes = c.recomputeTotalBytesLocked()
	return c.reconcileDBLocked()
}

func (c *cacheStore) reconcileDBLocked() error {
	expected := make(map[string]struct{}, len(c.entries))
	for key, e := range c.entries {
		expected[key] = struct{}{}
		if err := c.persistEntryLocked(key, e); err != nil {
			return err
		}
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		if b == nil {
			return nil
		}
		var stale [][]byte
		if err := b.ForEach(func(k, _ []byte) error {
			if _, ok := expected[string(k)]; !ok {
				stale = append(stale, append([]byte(nil), k...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range stale {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *cacheStore) persistEntryLocked(key string, e *entry) error {
	if e == nil {
		return nil
	}
	data, err := encodeEntry(e)
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		if b == nil {
			return fmt.Errorf("metadata bucket missing")
		}
		return b.Put([]byte(key), data)
	})
}

func (c *cacheStore) deleteEntryLocked(key string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		if b == nil {
			return fmt.Errorf("metadata bucket missing")
		}
		return b.Delete([]byte(key))
	})
}

func encodeEntry(e *entry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeEntry(data []byte) (*entry, error) {
	var e entry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&e); err != nil {
		return nil, err
	}
	return &e, nil
}
