package pagecache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testKey(hash uint64, pid uint64) pageKey {
	return pageKey{digestHash: hash, pageID: pid}
}

// ---------------------------------------------------------------------------
// Grant and Release lifecycle
// ---------------------------------------------------------------------------

func TestGrantAndRelease(t *testing.T) {
	var released []pageKey
	m := NewLeaseManager(time.Hour, func(key pageKey) {
		released = append(released, key)
	})

	key := testKey(1, 0)
	id := m.Grant("s1", key)

	if m.ActiveCount() != 1 {
		t.Fatalf("expected 1 active lease, got %d", m.ActiveCount())
	}

	if err := m.Release(id); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if m.ActiveCount() != 0 {
		t.Fatalf("expected 0 active leases, got %d", m.ActiveCount())
	}

	if len(released) != 1 || released[0] != key {
		t.Fatalf("onRelease not called correctly: %v", released)
	}

	// Releasing the same lease again should error.
	if err := m.Release(id); err == nil {
		t.Fatal("expected error releasing unknown lease")
	}
}

func TestGrantReturnsUniqueIDs(t *testing.T) {
	m := NewLeaseManager(time.Hour, nil)
	ids := make(map[uint64]struct{})
	for i := 0; i < 1000; i++ {
		id := m.Grant("s1", testKey(1, uint64(i)))
		if _, dup := ids[id]; dup {
			t.Fatalf("duplicate lease ID %d", id)
		}
		ids[id] = struct{}{}
	}
}

// ---------------------------------------------------------------------------
// Session cleanup (ReleaseAll)
// ---------------------------------------------------------------------------

func TestReleaseAll(t *testing.T) {
	var mu sync.Mutex
	releasedKeys := make(map[pageKey]int)
	m := NewLeaseManager(time.Hour, func(key pageKey) {
		mu.Lock()
		releasedKeys[key]++
		mu.Unlock()
	})

	// Grant several leases across two sessions.
	m.Grant("s1", testKey(1, 0))
	m.Grant("s1", testKey(1, 1))
	m.Grant("s2", testKey(2, 0))

	m.ReleaseAll("s1")

	if m.ActiveCount() != 1 {
		t.Fatalf("expected 1 remaining lease, got %d", m.ActiveCount())
	}

	if releasedKeys[testKey(1, 0)] != 1 || releasedKeys[testKey(1, 1)] != 1 {
		t.Fatalf("unexpected released keys: %v", releasedKeys)
	}

	// Calling ReleaseAll again for the same session should be a no-op.
	m.ReleaseAll("s1")
	if m.ActiveCount() != 1 {
		t.Fatalf("expected 1 remaining lease after duplicate ReleaseAll, got %d", m.ActiveCount())
	}
}

// ---------------------------------------------------------------------------
// BatchRelease
// ---------------------------------------------------------------------------

func TestBatchRelease(t *testing.T) {
	var count int32
	m := NewLeaseManager(time.Hour, func(_ pageKey) {
		atomic.AddInt32(&count, 1)
	})

	id1 := m.Grant("s1", testKey(1, 0))
	id2 := m.Grant("s1", testKey(1, 1))
	m.Grant("s1", testKey(1, 2)) // not released

	m.BatchRelease([]uint64{id1, id2, 9999}) // 9999 does not exist

	if m.ActiveCount() != 1 {
		t.Fatalf("expected 1 active lease, got %d", m.ActiveCount())
	}
	if atomic.LoadInt32(&count) != 2 {
		t.Fatalf("expected onRelease called 2 times, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Expired lease cleanup
// ---------------------------------------------------------------------------

func TestCleanExpired(t *testing.T) {
	var count int32
	m := NewLeaseManager(50*time.Millisecond, func(_ pageKey) {
		atomic.AddInt32(&count, 1)
	})

	m.Grant("s1", testKey(1, 0))
	m.Grant("s1", testKey(1, 1))

	// Nothing expired yet.
	m.CleanExpired()
	if m.ActiveCount() != 2 {
		t.Fatalf("expected 2 active leases, got %d", m.ActiveCount())
	}

	// Wait for leases to expire.
	time.Sleep(100 * time.Millisecond)

	m.CleanExpired()
	if m.ActiveCount() != 0 {
		t.Fatalf("expected 0 active leases, got %d", m.ActiveCount())
	}
	if atomic.LoadInt32(&count) != 2 {
		t.Fatalf("expected onRelease called 2 times, got %d", count)
	}
}

func TestStartCleanupLoop(t *testing.T) {
	var count int32
	m := NewLeaseManager(10*time.Millisecond, func(_ pageKey) {
		atomic.AddInt32(&count, 1)
	})

	m.Grant("s1", testKey(1, 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.StartCleanupLoop(ctx, 20*time.Millisecond)

	// Give the loop time to fire at least once after the lease expires.
	time.Sleep(150 * time.Millisecond)

	if m.ActiveCount() != 0 {
		t.Fatalf("expected 0 active leases, got %d", m.ActiveCount())
	}
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("expected onRelease called once, got %d", count)
	}

	cancel()
}

// ---------------------------------------------------------------------------
// Concurrent grant/release safety
// ---------------------------------------------------------------------------

func TestConcurrentGrantRelease(t *testing.T) {
	m := NewLeaseManager(time.Hour, func(_ pageKey) {})

	const goroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			session := "s"
			for i := 0; i < opsPerGoroutine; i++ {
				id := m.Grant(session, testKey(1, uint64(i)))
				_ = m.Release(id)
			}
		}(g)
	}

	wg.Wait()

	if m.ActiveCount() != 0 {
		t.Fatalf("expected 0 active leases after concurrent ops, got %d", m.ActiveCount())
	}
}

func TestConcurrentReleaseAll(t *testing.T) {
	m := NewLeaseManager(time.Hour, func(_ pageKey) {})

	// Pre-populate leases for multiple sessions.
	for i := 0; i < 100; i++ {
		m.Grant("s1", testKey(1, uint64(i)))
		m.Grant("s2", testKey(2, uint64(i)))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); m.ReleaseAll("s1") }()
	go func() { defer wg.Done(); m.ReleaseAll("s2") }()
	wg.Wait()

	if m.ActiveCount() != 0 {
		t.Fatalf("expected 0 active leases, got %d", m.ActiveCount())
	}
}
