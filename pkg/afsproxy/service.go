package afsproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/afsletpb"
)

const (
	defaultDialTimeout   = 3 * time.Second
	defaultStatusTimeout = 2 * time.Second
	defaultBackoff       = 50 * time.Millisecond
)

var (
	errNoAfsletResolved = errors.New("no afslet endpoints resolved")
	errNoCapacityNow    = errors.New("no afslet currently has enough available resources")
	errNoCapacityEver   = errors.New("requested resources exceed all afslet service limits")
)

type Config struct {
	AfsletTarget      string
	ProxyPeersTarget  string
	NodeID            string
	DialTimeout       time.Duration
	StatusTimeout     time.Duration
	DefaultBackoff    time.Duration
	HTTPClientTimeout time.Duration
}

type Service struct {
	afsletpb.UnimplementedAfsletServer

	afsletTarget      string
	proxyPeersTarget  string
	nodeID            string
	dialTimeout       time.Duration
	statusTimeout     time.Duration
	defaultBackoff    time.Duration
	httpClientTimeout time.Duration

	dispatching atomic.Int64

	randMu sync.Mutex
	rand   *rand.Rand
}

type backendCandidate struct {
	address string
	status  *afsletpb.GetRuntimeStatusResponse
}

type localDispatchResponse struct {
	NodeID           string `json:"node_id"`
	LocalDispatching int64  `json:"local_dispatching"`
}

type dispatchStatusResponse struct {
	NodeID             string `json:"node_id"`
	IncludeCluster     bool   `json:"include_cluster"`
	LocalDispatching   int64  `json:"local_dispatching"`
	ClusterDispatching int64  `json:"cluster_dispatching"`
	QueriedPeers       int    `json:"queried_peers"`
	PeerErrors         int    `json:"peer_errors"`
}

func NewService(cfg Config) *Service {
	s := &Service{
		afsletTarget:      strings.TrimSpace(cfg.AfsletTarget),
		proxyPeersTarget:  strings.TrimSpace(cfg.ProxyPeersTarget),
		nodeID:            strings.TrimSpace(cfg.NodeID),
		dialTimeout:       cfg.DialTimeout,
		statusTimeout:     cfg.StatusTimeout,
		defaultBackoff:    cfg.DefaultBackoff,
		httpClientTimeout: cfg.HTTPClientTimeout,
		rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if s.afsletTarget == "" {
		s.afsletTarget = "127.0.0.1:61051"
	}
	if s.nodeID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "afs-proxy"
		}
		s.nodeID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if s.dialTimeout <= 0 {
		s.dialTimeout = defaultDialTimeout
	}
	if s.statusTimeout <= 0 {
		s.statusTimeout = defaultStatusTimeout
	}
	if s.defaultBackoff <= 0 {
		s.defaultBackoff = defaultBackoff
	}
	if s.httpClientTimeout <= 0 {
		s.httpClientTimeout = s.statusTimeout
	}
	return s
}

func (s *Service) Execute(stream afsletpb.Afslet_ExecuteServer) error {
	firstReq, recvErr := stream.Recv()
	if recvErr == io.EOF {
		return status.Error(codes.InvalidArgument, "missing start request")
	}
	if recvErr != nil {
		return recvErr
	}
	startPayload, ok := firstReq.GetPayload().(*afsletpb.ExecuteRequest_Start)
	if !ok || startPayload.Start == nil {
		return status.Error(codes.InvalidArgument, "first request must be start")
	}
	start := startPayload.Start
	cpu := start.GetCpuCores()
	if cpu <= 0 {
		return status.Error(codes.InvalidArgument, "start.cpu_cores must be > 0")
	}
	memoryMB := start.GetMemoryMb()
	if memoryMB <= 0 {
		return status.Error(codes.InvalidArgument, "start.memory_mb must be > 0")
	}
	if strings.TrimSpace(start.GetImage()) == "" {
		return status.Error(codes.InvalidArgument, "start.image is required")
	}
	if len(start.GetCommand()) == 0 {
		return status.Error(codes.InvalidArgument, "start.command is required")
	}

	maxRetries := start.GetProxyDispatchMaxRetries()
	if maxRetries < 0 {
		return status.Error(codes.InvalidArgument, "start.proxy_dispatch_max_retries must be >= 0")
	}
	backoff := time.Duration(start.GetProxyDispatchBackoffMs()) * time.Millisecond
	if backoff <= 0 {
		backoff = s.defaultBackoff
	}

	s.dispatching.Add(1)
	defer s.dispatching.Add(-1)

	var failures int64
	var backendConn *grpc.ClientConn
	var backendStream afsletpb.Afslet_ExecuteClient
	var acceptedResp *afsletpb.ExecuteResponse
	for {
		candidate, selErr := s.selectCandidate(stream.Context(), cpu, memoryMB)
		if selErr == nil {
			conn, down, accepted, runErr := s.dispatchHandshake(stream.Context(), candidate.address, firstReq)
			if runErr == nil {
				backendConn = conn
				backendStream = down
				acceptedResp = accepted
				break
			}
			if conn != nil {
				_ = conn.Close()
			}
			if !isRetryableDispatchError(runErr) {
				return runErr
			}
			selErr = runErr
		}

		if errors.Is(selErr, errNoCapacityEver) {
			return status.Errorf(codes.InvalidArgument, "cannot run request cpu=%d memory_mb=%d: %v", cpu, memoryMB, selErr)
		}
		failures++
		if maxRetries > 0 && failures > maxRetries {
			return status.Errorf(codes.Unavailable, "dispatch retries exhausted after %d failures: %v", failures, selErr)
		}
		if waitErr := sleepWithContext(stream.Context(), s.computeBackoff(backoff, failures)); waitErr != nil {
			return waitErr
		}
	}
	defer backendConn.Close()

	if err := stream.Send(acceptedResp); err != nil {
		return err
	}

	uploadErrCh := make(chan error, 1)
	go func() {
		uploadErrCh <- forwardClientRequests(stream, backendStream)
	}()

	for {
		resp, err := backendStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if sendErr := stream.Send(resp); sendErr != nil {
			return sendErr
		}
	}
	if err := <-uploadErrCh; err != nil {
		return err
	}
	return nil
}

func (s *Service) GetRuntimeStatus(ctx context.Context, req *afsletpb.GetRuntimeStatusRequest) (*afsletpb.GetRuntimeStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "afs_proxy does not implement GetRuntimeStatus")
}

func (s *Service) HandleDispatchingHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/dispatching" {
		http.NotFound(w, r)
		return
	}
	includeCluster := parseBoolQuery(r.URL.Query().Get("include_cluster"))

	local := s.dispatching.Load()
	resp := dispatchStatusResponse{
		NodeID:           s.nodeID,
		IncludeCluster:   includeCluster,
		LocalDispatching: local,
	}
	if !includeCluster {
		resp.ClusterDispatching = local
		writeJSON(w, http.StatusOK, resp)
		return
	}
	if strings.TrimSpace(s.proxyPeersTarget) == "" {
		resp.ClusterDispatching = local
		writeJSON(w, http.StatusOK, resp)
		return
	}

	totalByNode := map[string]int64{s.nodeID: local}
	peers, err := resolveHostPorts(r.Context(), s.proxyPeersTarget)
	if err != nil {
		resp.ClusterDispatching = local
		resp.PeerErrors = 1
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.QueriedPeers = len(peers)

	client := &http.Client{Timeout: s.httpClientTimeout}
	for _, peer := range peers {
		peerResp, perr := s.queryPeerDispatching(r.Context(), client, peer)
		if perr != nil {
			resp.PeerErrors++
			continue
		}
		totalByNode[peerResp.NodeID] = peerResp.LocalDispatching
	}
	for _, v := range totalByNode {
		resp.ClusterDispatching += v
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) queryPeerDispatching(ctx context.Context, client *http.Client, peerAddr string) (*localDispatchResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+peerAddr+"/dispatching?include_cluster=false", nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer status=%d", res.StatusCode)
	}
	var out localDispatchResponse
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.NodeID) == "" {
		return nil, fmt.Errorf("peer response missing node_id")
	}
	return &out, nil
}

func (s *Service) selectCandidate(ctx context.Context, cpu int64, memoryMB int64) (*backendCandidate, error) {
	addresses, err := resolveHostPorts(ctx, s.afsletTarget)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, errNoAfsletResolved
	}

	candidates := make([]backendCandidate, 0, len(addresses))
	maxCPU := int64(0)
	maxMem := int64(0)
	observed := 0
	for _, addr := range addresses {
		st, err := s.fetchRuntimeStatus(ctx, addr)
		if err != nil {
			continue
		}
		observed++
		if st.GetLimitCpuCores() > maxCPU {
			maxCPU = st.GetLimitCpuCores()
		}
		if st.GetLimitMemoryMb() > maxMem {
			maxMem = st.GetLimitMemoryMb()
		}
		if st.GetLimitCpuCores() < cpu || st.GetLimitMemoryMb() < memoryMB {
			continue
		}
		if st.GetAvailableCpuCores() < cpu || st.GetAvailableMemoryMb() < memoryMB {
			continue
		}
		candidates = append(candidates, backendCandidate{address: addr, status: st})
	}

	if len(candidates) == 0 {
		if observed > 0 && (maxCPU < cpu || maxMem < memoryMB) {
			return nil, errNoCapacityEver
		}
		return nil, errNoCapacityNow
	}
	picked := s.pickByPowerOfTwoChoices(candidates)
	return &picked, nil
}

func (s *Service) pickByPowerOfTwoChoices(candidates []backendCandidate) backendCandidate {
	if len(candidates) == 1 {
		return candidates[0]
	}
	i := s.randIndex(len(candidates))
	j := s.randIndex(len(candidates) - 1)
	if j >= i {
		j++
	}
	a := candidates[i]
	b := candidates[j]
	if candidateLoad(a) <= candidateLoad(b) {
		return a
	}
	return b
}

func candidateLoad(c backendCandidate) float64 {
	cpuLoad := ratio(c.status.GetUsedCpuCores(), c.status.GetLimitCpuCores())
	memLoad := ratio(c.status.GetUsedMemoryMb(), c.status.GetLimitMemoryMb())
	if cpuLoad > memLoad {
		return cpuLoad
	}
	return memLoad
}

func ratio(num int64, den int64) float64 {
	if den <= 0 {
		return math.Inf(1)
	}
	if num <= 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func (s *Service) randIndex(n int) int {
	s.randMu.Lock()
	defer s.randMu.Unlock()
	return s.rand.Intn(n)
}

func (s *Service) fetchRuntimeStatus(ctx context.Context, addr string) (*afsletpb.GetRuntimeStatusResponse, error) {
	dialCtx, dialCancel := context.WithTimeout(ctx, s.dialTimeout)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := afsletpb.NewAfsletClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, s.statusTimeout)
	defer callCancel()
	return client.GetRuntimeStatus(callCtx, &afsletpb.GetRuntimeStatusRequest{})
}

func (s *Service) dispatchHandshake(ctx context.Context, backendAddr string, startReq *afsletpb.ExecuteRequest) (*grpc.ClientConn, afsletpb.Afslet_ExecuteClient, *afsletpb.ExecuteResponse, error) {
	dialCtx, dialCancel := context.WithTimeout(ctx, s.dialTimeout)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, nil, err
	}

	backend := afsletpb.NewAfsletClient(conn)
	backendStream, err := backend.Execute(ctx)
	if err != nil {
		return conn, nil, nil, err
	}
	if err := backendStream.Send(startReq); err != nil {
		return conn, nil, nil, err
	}

	firstResp, err := backendStream.Recv()
	if err != nil {
		return conn, nil, nil, err
	}
	acc, ok := firstResp.GetPayload().(*afsletpb.ExecuteResponse_Accepted)
	if !ok || !acc.Accepted.GetAccepted() {
		return conn, nil, nil, fmt.Errorf("backend %s did not return accepted marker", backendAddr)
	}
	return conn, backendStream, firstResp, nil
}

func forwardClientRequests(clientStream afsletpb.Afslet_ExecuteServer, backendStream afsletpb.Afslet_ExecuteClient) error {
	for {
		req, err := clientStream.Recv()
		if err == io.EOF {
			return backendStream.CloseSend()
		}
		if err != nil {
			return err
		}
		if sendErr := backendStream.Send(req); sendErr != nil {
			return sendErr
		}
	}
}

func isRetryableDispatchError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unimplemented:
		return false
	default:
		return true
	}
}

func resolveHostPorts(ctx context.Context, target string) ([]string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, errNoAfsletResolved
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		return []string{target}, nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		if strings.TrimSpace(ip.IP.String()) == "" {
			continue
		}
		addr := net.JoinHostPort(ip.IP.String(), port)
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	if len(out) == 0 {
		return nil, errNoAfsletResolved
	}
	return out, nil
}

func parseBoolQuery(v string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err == nil {
		return b
	}
	return strings.TrimSpace(v) == "1"
}

func (s *Service) computeBackoff(base time.Duration, failures int64) time.Duration {
	if base <= 0 {
		base = defaultBackoff
	}
	linear := time.Duration(failures) * base
	jitter := time.Duration(s.randIndex(int(base.Microseconds()+1))) * time.Microsecond
	return linear + jitter
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(v)
}
