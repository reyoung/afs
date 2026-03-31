package pagecache

import (
	"math/bits"
	"sync"
)

// ---------------------------------------------------------------------------
// Count-Min Sketch with 4-bit counters
// ---------------------------------------------------------------------------

const cmDepth = 4

// countMinSketch is a probabilistic frequency counter using 4-bit counters
// packed into uint8 values (two counters per byte).
type countMinSketch struct {
	width    int
	depth    int // always cmDepth
	counters [][]uint8
	total    int64
	decayAt  int64
}

func newCountMinSketch(width int, decayPeriod int64) *countMinSketch {
	// Round width up to the next power of two for fast modulo via bitmask.
	w := nextPowerOf2(width)
	s := &countMinSketch{
		width:    w,
		depth:    cmDepth,
		counters: make([][]uint8, cmDepth),
		decayAt:  decayPeriod,
	}
	// Each row stores w 4-bit counters packed into w/2 bytes.
	byteWidth := w / 2
	if byteWidth == 0 {
		byteWidth = 1
	}
	for i := range s.counters {
		s.counters[i] = make([]uint8, byteWidth)
	}
	return s
}

// increment records one access for the given hash value.
func (s *countMinSketch) increment(hash uint64) {
	for i := 0; i < cmDepth; i++ {
		idx := s.index(hash, i)
		s.counterAdd(i, idx, 1)
	}
	s.total++
	if s.total >= s.decayAt {
		s.decay()
	}
}

// estimate returns the minimum counter value across all rows for hash.
func (s *countMinSketch) estimate(hash uint64) uint8 {
	var min uint8 = 15
	for i := 0; i < cmDepth; i++ {
		idx := s.index(hash, i)
		v := s.counterGet(i, idx)
		if v < min {
			min = v
		}
	}
	return min
}

// decay halves all counters and resets the total.
func (s *countMinSketch) decay() {
	for i := 0; i < cmDepth; i++ {
		for j := range s.counters[i] {
			// Each byte holds two 4-bit counters: high nibble and low nibble.
			// Halving each nibble independently:
			//   high = (byte >> 4) >> 1 = byte >> 5
			//   low  = ((byte & 0x0F) >> 1)
			b := s.counters[i][j]
			hi := (b >> 5) & 0x07
			lo := (b & 0x0F) >> 1
			s.counters[i][j] = (hi << 4) | lo
		}
	}
	s.total = 0
}

// index returns the column index for a given hash and row.
func (s *countMinSketch) index(hash uint64, row int) int {
	// Derive independent hashes by splitting and mixing.
	h := hash
	switch row {
	case 0:
		// Use lower 32 bits mixed.
		h = hash * 0x9E3779B97F4A7C15
	case 1:
		h = (hash >> 16) * 0xBF58476D1CE4E5B9
	case 2:
		h = (hash >> 32) * 0x94D049BB133111EB
	case 3:
		h = bits.RotateLeft64(hash, 17) * 0x517CC1B727220A95
	}
	// Finalise with xor-shift.
	h ^= h >> 33
	h *= 0xFF51AFD7ED558CCD
	h ^= h >> 33
	return int(h) & (s.width - 1) // bitmask since width is power of 2
}

// counterGet returns the 4-bit counter at row[col].
func (s *countMinSketch) counterGet(row, col int) uint8 {
	byteIdx := col / 2
	if col%2 == 0 {
		return s.counters[row][byteIdx] & 0x0F
	}
	return s.counters[row][byteIdx] >> 4
}

// counterAdd adds delta to the 4-bit counter at row[col], capping at 15.
func (s *countMinSketch) counterAdd(row, col int, delta uint8) {
	byteIdx := col / 2
	if col%2 == 0 {
		lo := s.counters[row][byteIdx] & 0x0F
		hi := s.counters[row][byteIdx] & 0xF0
		v := lo + delta
		if v > 15 {
			v = 15
		}
		s.counters[row][byteIdx] = hi | v
	} else {
		lo := s.counters[row][byteIdx] & 0x0F
		hi := (s.counters[row][byteIdx] >> 4) + delta
		if hi > 15 {
			hi = 15
		}
		s.counters[row][byteIdx] = (hi << 4) | lo
	}
}

func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}

// ---------------------------------------------------------------------------
// Doubly-linked list (intrusive, for LRU chains)
// ---------------------------------------------------------------------------

type lruNode struct {
	key  pageKey
	prev *lruNode
	next *lruNode
}

type lruList struct {
	head   lruNode // sentinel
	tail   lruNode // sentinel
	length int
}

func (l *lruList) init() {
	l.head.next = &l.tail
	l.tail.prev = &l.head
	l.length = 0
}

func (l *lruList) pushFront(n *lruNode) {
	n.prev = &l.head
	n.next = l.head.next
	l.head.next.prev = n
	l.head.next = n
	l.length++
}

func (l *lruList) remove(n *lruNode) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
	l.length--
}

func (l *lruList) popBack() *lruNode {
	if l.length == 0 {
		return nil
	}
	n := l.tail.prev
	l.remove(n)
	return n
}

func (l *lruList) moveToFront(n *lruNode) {
	l.remove(n)
	l.pushFront(n)
}

// ---------------------------------------------------------------------------
// pageKey and pageLocation
// ---------------------------------------------------------------------------

type pageKey struct {
	digestHash uint64
	pageID     uint64
}

type pageLocation int

const (
	windowLRU pageLocation = iota
	probation
	protected
)

// ---------------------------------------------------------------------------
// W-TinyLFU eviction policy
// ---------------------------------------------------------------------------

type evictionPolicy struct {
	sketch    *countMinSketch
	window    lruList
	probation lruList
	protected lruList

	windowCap    int
	probationCap int
	protectedCap int

	nodeLocation map[pageKey]pageLocation
	nodes        map[pageKey]*lruNode

	mu sync.Mutex
}

func newEvictionPolicy(maxPages int) *evictionPolicy {
	if maxPages < 1 {
		maxPages = 1
	}

	// Window: ~1% of capacity (at least 1).
	windowCap := maxPages / 100
	if windowCap < 1 {
		windowCap = 1
	}

	mainCap := maxPages - windowCap
	if mainCap < 1 {
		mainCap = 1
	}

	// Protected: ~80% of main, probation: ~20% of main.
	protectedCap := mainCap * 80 / 100
	probationCap := mainCap - protectedCap
	if probationCap < 1 {
		probationCap = 1
	}

	// Sketch width: 10× maxPages for good accuracy; decay every maxPages accesses.
	sketchWidth := maxPages * 10
	if sketchWidth < 16 {
		sketchWidth = 16
	}
	decayPeriod := int64(maxPages)
	if decayPeriod < 100 {
		decayPeriod = 100
	}

	e := &evictionPolicy{
		sketch:       newCountMinSketch(sketchWidth, decayPeriod),
		windowCap:    windowCap,
		probationCap: probationCap,
		protectedCap: protectedCap,
		nodeLocation: make(map[pageKey]pageLocation),
		nodes:        make(map[pageKey]*lruNode),
	}
	e.window.init()
	e.probation.init()
	e.protected.init()
	return e
}

// Access records an access for key and promotes it within the eviction lists.
func (e *evictionPolicy) Access(key pageKey) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.sketch.increment(key.digestHash ^ key.pageID)

	n, ok := e.nodes[key]
	if !ok {
		return
	}

	loc := e.nodeLocation[key]
	switch loc {
	case windowLRU:
		e.window.moveToFront(n)

	case probation:
		// Promote from probation to protected.
		e.probation.remove(n)
		e.protected.pushFront(n)
		e.nodeLocation[key] = protected

		// If protected overflows, demote its tail to probation.
		for e.protected.length > e.protectedCap {
			demoted := e.protected.popBack()
			if demoted != nil {
				e.probation.pushFront(demoted)
				e.nodeLocation[demoted.key] = probation
			}
		}

	case protected:
		e.protected.moveToFront(n)
	}
}

// Add inserts a new entry into the eviction policy. It returns a victim to
// evict if capacity is exceeded, or nil if no eviction is needed.
func (e *evictionPolicy) Add(key pageKey) (evicted *pageKey) {
	return e.addInternal(key, nil)
}

func (e *evictionPolicy) AddPinned(key pageKey, isPinned func(pageKey) bool) (evicted *pageKey) {
	return e.addInternal(key, isPinned)
}

func (e *evictionPolicy) addInternal(key pageKey, isPinned func(pageKey) bool) (evicted *pageKey) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.nodes[key]; exists {
		// Already tracked; treat as access.
		e.mu.Unlock()
		e.Access(key)
		e.mu.Lock()
		return nil
	}

	e.sketch.increment(key.digestHash ^ key.pageID)

	n := &lruNode{key: key}
	e.nodes[key] = n
	e.nodeLocation[key] = windowLRU
	e.window.pushFront(n)

	if e.window.length <= e.windowCap {
		return nil
	}

	// Window is full; evict the window tail.
	windowVictim := e.window.popBack()
	if windowVictim == nil {
		return nil
	}

	// Move window victim to probation.
	delete(e.nodeLocation, windowVictim.key)

	// Compare with probation tail (if probation is at capacity).
	if e.probation.length < e.probationCap {
		e.probation.pushFront(windowVictim)
		e.nodeLocation[windowVictim.key] = probation
		return nil
	}

	// Probation at capacity; compare frequencies.
	probationVictim := e.probation.popBack()
	if probationVictim == nil {
		// Shouldn't happen, but handle gracefully.
		e.probation.pushFront(windowVictim)
		e.nodeLocation[windowVictim.key] = probation
		return nil
	}
	delete(e.nodeLocation, probationVictim.key)

	wFreq := e.sketch.estimate(windowVictim.key.digestHash ^ windowVictim.key.pageID)
	pFreq := e.sketch.estimate(probationVictim.key.digestHash ^ probationVictim.key.pageID)
	windowPinned := isPinned != nil && isPinned(windowVictim.key)
	probationPinned := isPinned != nil && isPinned(probationVictim.key)

	if windowPinned && probationPinned {
		e.probation.pushFront(probationVictim)
		e.nodeLocation[probationVictim.key] = probation
		e.probation.pushFront(windowVictim)
		e.nodeLocation[windowVictim.key] = probation
		return nil
	}
	if windowPinned {
		e.probation.pushFront(windowVictim)
		e.nodeLocation[windowVictim.key] = probation
		delete(e.nodes, probationVictim.key)
		evictedKey := probationVictim.key
		return &evictedKey
	}
	if probationPinned {
		e.probation.pushFront(probationVictim)
		e.nodeLocation[probationVictim.key] = probation
		delete(e.nodes, windowVictim.key)
		evictedKey := windowVictim.key
		return &evictedKey
	}

	if wFreq > pFreq {
		// Window victim wins; admit to probation. Probation victim is evicted.
		e.probation.pushFront(windowVictim)
		e.nodeLocation[windowVictim.key] = probation
		delete(e.nodes, probationVictim.key)
		evictedKey := probationVictim.key
		return &evictedKey
	}

	// Probation victim wins (or tie); keep probation victim, evict window victim.
	e.probation.pushFront(probationVictim)
	e.nodeLocation[probationVictim.key] = probation
	delete(e.nodes, windowVictim.key)
	evictedKey := windowVictim.key
	return &evictedKey
}

// Remove removes an entry from eviction tracking.
func (e *evictionPolicy) Remove(key pageKey) {
	e.mu.Lock()
	defer e.mu.Unlock()

	n, ok := e.nodes[key]
	if !ok {
		return
	}

	loc := e.nodeLocation[key]
	switch loc {
	case windowLRU:
		e.window.remove(n)
	case probation:
		e.probation.remove(n)
	case protected:
		e.protected.remove(n)
	}

	delete(e.nodes, key)
	delete(e.nodeLocation, key)
}

// FindVictim finds an evictable victim that is not pinned. It scans the
// probation tail first, then the window tail.
func (e *evictionPolicy) FindVictim(isPinned func(pageKey) bool) *pageKey {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Scan probation from tail.
	if victim := e.scanForVictim(&e.probation, isPinned); victim != nil {
		return victim
	}
	// Scan window from tail.
	if victim := e.scanForVictim(&e.window, isPinned); victim != nil {
		return victim
	}
	// Scan protected from tail as last resort.
	if victim := e.scanForVictim(&e.protected, isPinned); victim != nil {
		return victim
	}
	return nil
}

// scanForVictim walks a list from tail to head looking for an unpinned node.
func (e *evictionPolicy) scanForVictim(list *lruList, isPinned func(pageKey) bool) *pageKey {
	cur := list.tail.prev
	for cur != &list.head {
		if !isPinned(cur.key) {
			k := cur.key
			return &k
		}
		cur = cur.prev
	}
	return nil
}
