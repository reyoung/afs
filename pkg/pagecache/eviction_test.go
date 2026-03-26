package pagecache

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Count-Min Sketch tests
// ---------------------------------------------------------------------------

func TestCountMinSketch_IncrementAndEstimate(t *testing.T) {
	s := newCountMinSketch(256, 1000)

	s.increment(42)
	s.increment(42)
	s.increment(42)

	est := s.estimate(42)
	if est < 3 {
		t.Fatalf("expected estimate >= 3, got %d", est)
	}

	// An unseen key should have estimate 0.
	if got := s.estimate(999999); got != 0 {
		t.Fatalf("expected estimate 0 for unseen key, got %d", got)
	}
}

func TestCountMinSketch_CounterSaturation(t *testing.T) {
	s := newCountMinSketch(64, 100000) // large decay period to avoid decay

	for i := 0; i < 30; i++ {
		s.increment(7)
	}

	est := s.estimate(7)
	if est != 15 {
		t.Fatalf("expected saturated counter at 15, got %d", est)
	}
}

func TestCountMinSketch_Decay(t *testing.T) {
	s := newCountMinSketch(256, 100000)

	for i := 0; i < 10; i++ {
		s.increment(100)
	}

	before := s.estimate(100)
	s.decay()
	after := s.estimate(100)

	if after >= before {
		t.Fatalf("expected decay to reduce estimate: before=%d, after=%d", before, after)
	}
	if after != before/2 {
		t.Fatalf("expected halved counter: before=%d, after=%d", before, after)
	}
}

func TestCountMinSketch_AutoDecay(t *testing.T) {
	decayPeriod := int64(10)
	s := newCountMinSketch(256, decayPeriod)

	// Increment a key enough to trigger auto-decay.
	for i := int64(0); i < decayPeriod; i++ {
		s.increment(50)
	}

	// After decay, estimate should be halved from ~10 → ~5.
	est := s.estimate(50)
	if est > 7 {
		t.Fatalf("expected auto-decay to reduce estimate, got %d", est)
	}
}

// ---------------------------------------------------------------------------
// LRU list tests
// ---------------------------------------------------------------------------

func TestLRUList_PushFrontAndPopBack(t *testing.T) {
	var l lruList
	l.init()

	a := &lruNode{key: pageKey{digestHash: 1, pageID: 1}}
	b := &lruNode{key: pageKey{digestHash: 2, pageID: 2}}
	c := &lruNode{key: pageKey{digestHash: 3, pageID: 3}}

	l.pushFront(a)
	l.pushFront(b)
	l.pushFront(c)

	if l.length != 3 {
		t.Fatalf("expected length 3, got %d", l.length)
	}

	// Pop order should be a, b, c (FIFO from back).
	got := l.popBack()
	if got != a {
		t.Fatalf("expected node a, got %+v", got.key)
	}
	got = l.popBack()
	if got != b {
		t.Fatalf("expected node b, got %+v", got.key)
	}
	got = l.popBack()
	if got != c {
		t.Fatalf("expected node c, got %+v", got.key)
	}

	if l.popBack() != nil {
		t.Fatal("expected nil from empty list")
	}
}

func TestLRUList_Remove(t *testing.T) {
	var l lruList
	l.init()

	a := &lruNode{key: pageKey{digestHash: 1, pageID: 1}}
	b := &lruNode{key: pageKey{digestHash: 2, pageID: 2}}

	l.pushFront(a)
	l.pushFront(b)
	l.remove(a)

	if l.length != 1 {
		t.Fatalf("expected length 1, got %d", l.length)
	}
	got := l.popBack()
	if got != b {
		t.Fatalf("expected node b, got %+v", got.key)
	}
}

func TestLRUList_MoveToFront(t *testing.T) {
	var l lruList
	l.init()

	a := &lruNode{key: pageKey{digestHash: 1, pageID: 1}}
	b := &lruNode{key: pageKey{digestHash: 2, pageID: 2}}
	c := &lruNode{key: pageKey{digestHash: 3, pageID: 3}}

	l.pushFront(a)
	l.pushFront(b)
	l.pushFront(c)

	// Move a (currently at back) to front.
	l.moveToFront(a)

	// Now back should be b.
	got := l.popBack()
	if got != b {
		t.Fatalf("expected node b at back after move, got %+v", got.key)
	}
}

// ---------------------------------------------------------------------------
// W-TinyLFU eviction policy tests
// ---------------------------------------------------------------------------

func TestEvictionPolicy_AddAndEvict(t *testing.T) {
	// Small capacity to trigger evictions quickly.
	e := newEvictionPolicy(10)

	var evictedKeys []pageKey
	for i := uint64(0); i < 100; i++ {
		key := pageKey{digestHash: i, pageID: 0}
		if victim := e.Add(key); victim != nil {
			evictedKeys = append(evictedKeys, *victim)
		}
	}

	if len(evictedKeys) == 0 {
		t.Fatal("expected some evictions with 100 adds to capacity-10 policy")
	}

	// Total tracked nodes should not exceed capacity.
	e.mu.Lock()
	tracked := len(e.nodes)
	e.mu.Unlock()
	if tracked > 10 {
		t.Fatalf("expected at most 10 tracked nodes, got %d", tracked)
	}
}

func TestEvictionPolicy_Access_PromotesToProtected(t *testing.T) {
	e := newEvictionPolicy(200)

	key := pageKey{digestHash: 42, pageID: 0}
	e.Add(key)

	// Fill window to push our key to probation.
	for i := uint64(1); i <= uint64(e.windowCap)+5; i++ {
		e.Add(pageKey{digestHash: 1000 + i, pageID: 0})
	}

	// If key landed in probation, accessing it should promote to protected.
	e.Access(key)

	e.mu.Lock()
	loc, ok := e.nodeLocation[key]
	e.mu.Unlock()

	if ok && loc == probation {
		t.Fatal("expected key to be promoted from probation after access")
	}
}

func TestEvictionPolicy_Remove(t *testing.T) {
	e := newEvictionPolicy(100)

	key := pageKey{digestHash: 1, pageID: 1}
	e.Add(key)
	e.Remove(key)

	e.mu.Lock()
	_, tracked := e.nodes[key]
	e.mu.Unlock()

	if tracked {
		t.Fatal("expected key to be removed from tracking")
	}
}

func TestEvictionPolicy_FindVictim_SkipsPinned(t *testing.T) {
	e := newEvictionPolicy(100)

	pinned := pageKey{digestHash: 1, pageID: 0}
	unpinned := pageKey{digestHash: 2, pageID: 0}

	e.Add(pinned)
	e.Add(unpinned)

	victim := e.FindVictim(func(k pageKey) bool {
		return k == pinned
	})

	if victim == nil {
		t.Fatal("expected to find a victim")
	}
	if *victim == pinned {
		t.Fatal("expected victim to not be the pinned key")
	}
}

func TestEvictionPolicy_FindVictim_AllPinned(t *testing.T) {
	e := newEvictionPolicy(100)

	e.Add(pageKey{digestHash: 1, pageID: 0})
	e.Add(pageKey{digestHash: 2, pageID: 0})

	victim := e.FindVictim(func(k pageKey) bool {
		return true // everything pinned
	})

	if victim != nil {
		t.Fatal("expected nil victim when all entries are pinned")
	}
}

func TestEvictionPolicy_FrequencyComparison(t *testing.T) {
	// Verify that a frequently accessed entry survives over a less frequent one.
	e := newEvictionPolicy(10)

	hot := pageKey{digestHash: 1, pageID: 0}
	e.Add(hot)

	// Boost the frequency of the hot key.
	for i := 0; i < 20; i++ {
		e.Access(hot)
	}

	// Fill up the cache with cold keys.
	for i := uint64(100); i < 200; i++ {
		e.Add(pageKey{digestHash: i, pageID: 0})
	}

	// The hot key should still be tracked.
	e.mu.Lock()
	_, ok := e.nodes[hot]
	e.mu.Unlock()

	if !ok {
		t.Fatal("expected frequently accessed key to survive eviction")
	}
}

func TestEvictionPolicy_WindowToProbationPromotion(t *testing.T) {
	// With a moderately sized cache, adding more entries than the window
	// capacity should cause entries to flow into probation.
	e := newEvictionPolicy(500)

	for i := uint64(0); i < uint64(e.windowCap)+10; i++ {
		e.Add(pageKey{digestHash: i, pageID: 0})
	}

	e.mu.Lock()
	probLen := e.probation.length
	e.mu.Unlock()

	if probLen == 0 {
		t.Fatal("expected some entries in probation after overflowing window")
	}
}
