package layerformat

import "sync"

// cachedTOC holds the pre-parsed TOC data for a layer.
type cachedTOC struct {
	tocData    toc
	entriesByP map[string]Entry
	dataStart  int64
}

// TOCCache is a thread-safe LRU cache for parsed AFSLYR02 TOC metadata.
// Key is the layer digest (e.g. "sha256:abc123..."). Since layer content
// is immutable for a given digest, the parsed TOC can be safely reused
// across multiple execute sessions.
type TOCCache struct {
	mu       sync.Mutex
	entries  map[string]*cachedTOC
	order    []string // LRU: most recent at front
	maxItems int
}

// NewTOCCache creates a TOC cache with the given capacity.
func NewTOCCache(maxItems int) *TOCCache {
	if maxItems < 1 {
		maxItems = 1
	}
	return &TOCCache{
		entries:  make(map[string]*cachedTOC, maxItems),
		order:    make([]string, 0, maxItems),
		maxItems: maxItems,
	}
}

// Get retrieves a cached TOC by digest. Returns (cached, true) on hit.
func (c *TOCCache) Get(digest string) (*cachedTOC, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[digest]
	if !ok {
		return nil, false
	}
	// Move to front (LRU)
	c.moveToFront(digest)
	return entry, true
}

// Put stores a parsed TOC. Evicts the least recently used entry if at capacity.
func (c *TOCCache) Put(digest string, entry *cachedTOC) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.entries[digest]; ok {
		c.entries[digest] = entry
		c.moveToFront(digest)
		return
	}

	// Evict if at capacity
	for len(c.order) >= c.maxItems {
		victim := c.order[len(c.order)-1]
		c.order = c.order[:len(c.order)-1]
		delete(c.entries, victim)
	}

	c.entries[digest] = entry
	c.order = append([]string{digest}, c.order...)
}

// Len returns the number of cached entries.
func (c *TOCCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// moveToFront moves digest to front of the LRU order. Caller must hold mu.
func (c *TOCCache) moveToFront(digest string) {
	for i, d := range c.order {
		if d == digest {
			// Remove from current position
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	c.order = append([]string{digest}, c.order...)
}
