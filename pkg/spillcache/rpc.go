package spillcache

import (
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type PingRequest struct{}

type PingResponse struct {
	OK bool
}

type AcquireRequest struct {
	Digest   string
	FilePath string
}

type AcquireResponse struct {
	Hit        bool
	CachePath  string
	Size       int64
	LeaseToken string
	TempPath   string
}

type CommitRequest struct {
	LeaseToken string
}

type CommitResponse struct {
	CachePath string
	Size      int64
}

type AbortRequest struct {
	LeaseToken string
}

type AbortResponse struct{}

type rpcService struct {
	store *cacheStore
}

func (s *rpcService) Ping(_ *PingRequest, resp *PingResponse) error {
	resp.OK = true
	return nil
}

func (s *rpcService) Acquire(req *AcquireRequest, resp *AcquireResponse) error {
	path, size, lease, err := s.store.acquire(cacheKey{Digest: req.Digest, FilePath: req.FilePath})
	if err != nil {
		return err
	}
	if lease == nil {
		resp.Hit = true
		resp.CachePath = path
		resp.Size = size
		return nil
	}
	resp.Hit = false
	resp.LeaseToken = lease.Token
	resp.TempPath = lease.TempPath
	return nil
}

func (s *rpcService) Commit(req *CommitRequest, resp *CommitResponse) error {
	p, size, err := s.store.commit(req.LeaseToken)
	if err != nil {
		return err
	}
	resp.CachePath = p
	resp.Size = size
	return nil
}

func (s *rpcService) Abort(req *AbortRequest, _ *AbortResponse) error {
	return s.store.abort(req.LeaseToken)
}

type ServerConfig struct {
	CacheDir      string
	SockPath      string
	MaxBytes      int64
	OwnerLockPath string
	PIDFilePath   string
}

func RunServer(cfg ServerConfig) error {
	if strings.TrimSpace(cfg.SockPath) == "" {
		return fmt.Errorf("sock path is required")
	}
	if strings.TrimSpace(cfg.OwnerLockPath) == "" {
		cfg.OwnerLockPath = defaultOwnerLockPath(cfg.CacheDir)
	}
	if strings.TrimSpace(cfg.PIDFilePath) == "" {
		cfg.PIDFilePath = defaultPIDFilePath(cfg.CacheDir)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.SockPath), 0o755); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.OwnerLockPath), 0o755); err != nil {
		return fmt.Errorf("create owner lock dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.PIDFilePath), 0o755); err != nil {
		return fmt.Errorf("create pid file dir: %w", err)
	}
	lockFD, heldByOther, err := tryAcquireExclusiveFileLock(cfg.OwnerLockPath)
	if err != nil {
		return fmt.Errorf("acquire owner lock: %w", err)
	}
	if heldByOther {
		return fmt.Errorf("owner lock already held by another daemon: %s", cfg.OwnerLockPath)
	}
	defer releaseExclusiveFileLock(lockFD)

	store, err := newCacheStore(cfg.CacheDir, cfg.MaxBytes)
	if err != nil {
		return err
	}
	_ = os.Remove(cfg.SockPath)
	ln, err := net.Listen("unix", cfg.SockPath)
	if err != nil {
		return fmt.Errorf("listen on unix socket: %w", err)
	}
	defer ln.Close()
	if err := os.Chmod(cfg.SockPath, 0o666); err != nil {
		return fmt.Errorf("chmod socket: %w", err)
	}
	startTime, err := readProcessStartTime(os.Getpid())
	if err != nil {
		startTime = ""
	}
	if err := writeDaemonPIDState(cfg.PIDFilePath, daemonPIDState{
		PID:       os.Getpid(),
		StartTime: startTime,
	}); err != nil {
		return fmt.Errorf("write daemon pid file: %w", err)
	}
	defer os.Remove(cfg.PIDFilePath)

	srv := rpc.NewServer()
	if err := srv.RegisterName("SpillCache", &rpcService{store: store}); err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if err == io.EOF {
				return nil
			}
			return err
		}
		go srv.ServeConn(conn)
	}
}

type Client struct {
	sockPath string
	timeout  time.Duration
}

func NewClient(sockPath string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{sockPath: sockPath, timeout: timeout}
}

func (c *Client) call(method string, req interface{}, resp interface{}) error {
	dialer := net.Dialer{Timeout: c.timeout}
	conn, err := dialer.Dial("unix", c.sockPath)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	cl := rpc.NewClient(conn)
	defer cl.Close()
	return cl.Call(method, req, resp)
}

func (c *Client) Ping() error {
	var resp PingResponse
	if err := c.call("SpillCache.Ping", &PingRequest{}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("unexpected ping response")
	}
	return nil
}

func (c *Client) Prepare(digest string, filePath string, fill func(w io.Writer) (int64, error)) (string, int64, error) {
	var acq AcquireResponse
	if err := c.call("SpillCache.Acquire", &AcquireRequest{Digest: digest, FilePath: filePath}, &acq); err != nil {
		return "", 0, err
	}
	if acq.Hit {
		return acq.CachePath, acq.Size, nil
	}
	if strings.TrimSpace(acq.LeaseToken) == "" || strings.TrimSpace(acq.TempPath) == "" {
		return "", 0, fmt.Errorf("invalid acquire response")
	}

	f, err := os.OpenFile(acq.TempPath, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		_ = c.abort(acq.LeaseToken)
		return "", 0, err
	}
	n, err := fill(f)
	closeErr := f.Close()
	if err != nil {
		_ = c.abort(acq.LeaseToken)
		return "", n, err
	}
	if closeErr != nil {
		_ = c.abort(acq.LeaseToken)
		return "", n, closeErr
	}

	var commit CommitResponse
	if err := c.call("SpillCache.Commit", &CommitRequest{LeaseToken: acq.LeaseToken}, &commit); err != nil {
		_ = c.abort(acq.LeaseToken)
		return "", n, err
	}
	return commit.CachePath, commit.Size, nil
}

func (c *Client) abort(token string) error {
	if strings.TrimSpace(token) == "" {
		return nil
	}
	var resp AbortResponse
	return c.call("SpillCache.Abort", &AbortRequest{LeaseToken: token}, &resp)
}
