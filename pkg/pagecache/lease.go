package pagecache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Lease represents a time-bounded hold on a cached page. While a lease is
// active the page must remain pinned in the cache.
type Lease struct {
	ID        uint64
	SessionID string
	Key       pageKey
	CreatedAt time.Time
	ExpiresAt time.Time
}

// LeaseManager tracks outstanding leases and provides facilities for
// individual release, per-session bulk release, and periodic expiration.
type LeaseManager struct {
	mu            sync.Mutex
	leases        map[uint64]*Lease               // leaseID -> Lease
	sessionLeases map[string]map[uint64]struct{}   // sessionID -> set of leaseIDs
	nextID        uint64
	leaseTimeout  time.Duration

	// onRelease is called (under no lock) when a lease is released so that
	// the page cache can unpin the corresponding page.
	onRelease func(key pageKey)
}

// NewLeaseManager creates a LeaseManager with the given default lease
// duration and an optional callback that fires when a lease is released.
func NewLeaseManager(leaseTimeout time.Duration, onRelease func(key pageKey)) *LeaseManager {
	return &LeaseManager{
		leases:        make(map[uint64]*Lease),
		sessionLeases: make(map[string]map[uint64]struct{}),
		nextID:        1,
		leaseTimeout:  leaseTimeout,
		onRelease:     onRelease,
	}
}

// Grant creates a new lease for the given session and page key.
// It returns the unique lease ID.
func (m *LeaseManager) Grant(sessionID string, key pageKey) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	id := m.nextID
	m.nextID++

	lease := &Lease{
		ID:        id,
		SessionID: sessionID,
		Key:       key,
		CreatedAt: now,
		ExpiresAt: now.Add(m.leaseTimeout),
	}
	m.leases[id] = lease

	if m.sessionLeases[sessionID] == nil {
		m.sessionLeases[sessionID] = make(map[uint64]struct{})
	}
	m.sessionLeases[sessionID][id] = struct{}{}

	return id
}

// Release removes a single lease by ID and fires the onRelease callback.
// It returns an error if the lease does not exist.
func (m *LeaseManager) Release(leaseID uint64) error {
	m.mu.Lock()
	lease, ok := m.leases[leaseID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("lease %d not found", leaseID)
	}
	m.removeLeaseLocked(lease)
	m.mu.Unlock()

	if m.onRelease != nil {
		m.onRelease(lease.Key)
	}
	return nil
}

// ReleaseAll releases every lease belonging to the given session. This is
// useful for crash recovery: when a client session dies all of its leases
// should be cleaned up.
func (m *LeaseManager) ReleaseAll(sessionID string) {
	m.mu.Lock()
	ids, ok := m.sessionLeases[sessionID]
	if !ok {
		m.mu.Unlock()
		return
	}

	released := make([]*Lease, 0, len(ids))
	for id := range ids {
		if lease, exists := m.leases[id]; exists {
			released = append(released, lease)
			delete(m.leases, id)
		}
	}
	delete(m.sessionLeases, sessionID)
	m.mu.Unlock()

	if m.onRelease != nil {
		for _, lease := range released {
			m.onRelease(lease.Key)
		}
	}
}

// BatchRelease releases multiple leases in one call. Leases that do not
// exist are silently skipped.
func (m *LeaseManager) BatchRelease(leaseIDs []uint64) {
	m.mu.Lock()
	released := make([]*Lease, 0, len(leaseIDs))
	for _, id := range leaseIDs {
		if lease, ok := m.leases[id]; ok {
			released = append(released, lease)
			m.removeLeaseLocked(lease)
		}
	}
	m.mu.Unlock()

	if m.onRelease != nil {
		for _, lease := range released {
			m.onRelease(lease.Key)
		}
	}
}

// CleanExpired scans all leases and releases any that have passed their
// expiration time.
func (m *LeaseManager) CleanExpired() {
	now := time.Now()

	m.mu.Lock()
	var expired []*Lease
	for _, lease := range m.leases {
		if now.After(lease.ExpiresAt) {
			expired = append(expired, lease)
		}
	}
	for _, lease := range expired {
		m.removeLeaseLocked(lease)
	}
	m.mu.Unlock()

	if m.onRelease != nil {
		for _, lease := range expired {
			m.onRelease(lease.Key)
		}
	}
}

// ActiveCount returns the number of currently active leases.
func (m *LeaseManager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.leases)
}

// StartCleanupLoop runs a background goroutine that calls CleanExpired at
// the given interval. It stops when the context is cancelled.
func (m *LeaseManager) StartCleanupLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.CleanExpired()
			}
		}
	}()
}

// removeLeaseLocked deletes the lease from internal maps. The caller must
// hold m.mu.
func (m *LeaseManager) removeLeaseLocked(lease *Lease) {
	delete(m.leases, lease.ID)
	if ids, ok := m.sessionLeases[lease.SessionID]; ok {
		delete(ids, lease.ID)
		if len(ids) == 0 {
			delete(m.sessionLeases, lease.SessionID)
		}
	}
}
