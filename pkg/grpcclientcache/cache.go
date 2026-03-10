package grpcclientcache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Cache stores reusable gRPC client connections keyed by endpoint.
// The cache is bounded; once full, new endpoints are dialed as transient conns.
type Cache struct {
	mu         sync.RWMutex
	conns      map[string]*grpc.ClientConn
	maxEntries int
}

func New(maxEntries int) *Cache {
	if maxEntries <= 0 {
		maxEntries = 64
	}
	return &Cache{
		conns:      make(map[string]*grpc.ClientConn, maxEntries),
		maxEntries: maxEntries,
	}
}

// Acquire returns a connection and a release function.
// For cached connections, release is a no-op.
// For transient connections (cache full), release closes the conn.
func (c *Cache) Acquire(addr string, timeout time.Duration, insecureTransport bool) (*grpc.ClientConn, func(), error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, nil, fmt.Errorf("grpc address is required")
	}
	if !insecureTransport {
		return nil, nil, fmt.Errorf("secure transport is not implemented")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	key := cacheKey(addr, insecureTransport)
	if conn := c.getCached(key); conn != nil {
		return conn, func() {}, nil
	}

	conn, err := dial(addr, timeout)
	if err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existed, ok := c.conns[key]; ok && existed.GetState() != connectivity.Shutdown {
		_ = conn.Close()
		return existed, func() {}, nil
	}
	if len(c.conns) >= c.maxEntries {
		// Keep cache bounded: use transient conn and close on release.
		return conn, func() { _ = conn.Close() }, nil
	}
	c.conns[key] = conn
	return conn, func() {}, nil
}

func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, k)
	}
	return nil
}

func (c *Cache) getCached(key string) *grpc.ClientConn {
	c.mu.RLock()
	conn := c.conns[key]
	c.mu.RUnlock()
	if conn == nil {
		return nil
	}
	if conn.GetState() == connectivity.Shutdown {
		c.mu.Lock()
		if cur := c.conns[key]; cur == conn {
			delete(c.conns, key)
		}
		c.mu.Unlock()
		return nil
	}
	return conn
}

func cacheKey(addr string, insecureTransport bool) string {
	return fmt.Sprintf("%t|%s", insecureTransport, addr)
}

func dial(addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}
