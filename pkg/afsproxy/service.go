package afsproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
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
	"github.com/reyoung/afs/pkg/afsproxypb"
	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

const (
	defaultDialTimeout   = 3 * time.Second
	defaultStatusTimeout = 2 * time.Second
	defaultBackoff       = 50 * time.Millisecond
	defaultPlatformOS    = "linux"
	defaultPlatformArch  = "amd64"
	reconcilePollTimeout = 5 * time.Second
	pullImageRPCTimeout  = 10 * time.Minute
)

var (
	errNoAfsletResolved = errors.New("no afslet endpoints resolved")
	errNoCapacityNow    = errors.New("no afslet currently has enough available resources")
	errNoCapacityEver   = errors.New("requested resources exceed all afslet service limits")
)

type Config struct {
	AfsletTarget      string
	ProxyPeersTarget  string
	DiscoveryTarget   string
	NodeID            string
	FormatVersion     int
	DialTimeout       time.Duration
	StatusTimeout     time.Duration
	DefaultBackoff    time.Duration
	HTTPClientTimeout time.Duration
}

type Service struct {
	afsletpb.UnimplementedAfsletServer
	afsproxypb.UnimplementedAfsProxyServer

	afsletTarget      string
	proxyPeersTarget  string
	discoveryTarget   string
	nodeID            string
	formatVersion     layerformat.FormatVersion
	dialTimeout       time.Duration
	statusTimeout     time.Duration
	defaultBackoff    time.Duration
	httpClientTimeout time.Duration

	dispatching atomic.Int64

	randMu sync.Mutex
	rand   *rand.Rand

	requestSeq atomic.Int64
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

func availableCacheBytes(svc *discoverypb.ServiceInstance) int64 {
	if svc == nil {
		return 0
	}
	max := svc.GetCacheMaxBytes()
	if max <= 0 {
		return 0
	}
	var used int64
	for _, ls := range svc.GetLayerStats() {
		used += ls.GetAfsSize()
	}
	available := max - used
	if available < 0 {
		return 0
	}
	return available
}

func sortCandidatesByAvailableCache(candidates []string, svcByEndpoint map[string]*discoverypb.ServiceInstance) {
	sort.SliceStable(candidates, func(i, j int) bool {
		left := availableCacheBytes(svcByEndpoint[candidates[i]])
		right := availableCacheBytes(svcByEndpoint[candidates[j]])
		return left > right
	})
}

func NewService(cfg Config) *Service {
	s := &Service{
		afsletTarget:      strings.TrimSpace(cfg.AfsletTarget),
		proxyPeersTarget:  strings.TrimSpace(cfg.ProxyPeersTarget),
		discoveryTarget:   strings.TrimSpace(cfg.DiscoveryTarget),
		nodeID:            strings.TrimSpace(cfg.NodeID),
		formatVersion:     layerformat.FormatVersion(cfg.FormatVersion),
		dialTimeout:       cfg.DialTimeout,
		statusTimeout:     cfg.StatusTimeout,
		defaultBackoff:    cfg.DefaultBackoff,
		httpClientTimeout: cfg.HTTPClientTimeout,
		rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if s.formatVersion != layerformat.FormatV1 && s.formatVersion != layerformat.FormatV2 {
		s.formatVersion = layerformat.FormatV2
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
	reqID := s.requestSeq.Add(1)
	logPrefix := fmt.Sprintf("[proxy-exec req=%d]", reqID)

	firstReq, recvErr := stream.Recv()
	if recvErr == io.EOF {
		log.Printf("%s missing first frame: eof", logPrefix)
		return status.Error(codes.InvalidArgument, "missing start request")
	}
	if recvErr != nil {
		log.Printf("%s failed to recv first frame: %v", logPrefix, recvErr)
		return recvErr
	}
	startPayload, ok := firstReq.GetPayload().(*afsletpb.ExecuteRequest_Start)
	if !ok || startPayload.Start == nil {
		log.Printf("%s invalid first frame: not start", logPrefix)
		return status.Error(codes.InvalidArgument, "first request must be start")
	}
	start := startPayload.Start
	cpu := start.GetCpuCores()
	if cpu <= 0 {
		log.Printf("%s invalid cpu_cores=%d", logPrefix, cpu)
		return status.Error(codes.InvalidArgument, "start.cpu_cores must be > 0")
	}
	memoryMB := start.GetMemoryMb()
	if memoryMB <= 0 {
		log.Printf("%s invalid memory_mb=%d", logPrefix, memoryMB)
		return status.Error(codes.InvalidArgument, "start.memory_mb must be > 0")
	}
	if strings.TrimSpace(start.GetImage()) == "" {
		log.Printf("%s missing image", logPrefix)
		return status.Error(codes.InvalidArgument, "start.image is required")
	}

	maxRetries := start.GetProxyDispatchMaxRetries()
	if maxRetries < 0 {
		log.Printf("%s invalid max_retries=%d", logPrefix, maxRetries)
		return status.Error(codes.InvalidArgument, "start.proxy_dispatch_max_retries must be >= 0")
	}
	backoff := time.Duration(start.GetProxyDispatchBackoffMs()) * time.Millisecond
	if backoff <= 0 {
		backoff = s.defaultBackoff
	}
	log.Printf("%s start image=%s tag=%s cpu=%d memory_mb=%d cmd=%q max_retries=%d backoff=%s", logPrefix, start.GetImage(), start.GetTag(), cpu, memoryMB, start.GetCommand(), maxRetries, backoff)

	s.dispatching.Add(1)
	defer s.dispatching.Add(-1)

	var failures int64
	var backendConn *grpc.ClientConn
	var backendStream afsletpb.Afslet_ExecuteClient
	var acceptedResp *afsletpb.ExecuteResponse
	for {
		attempt := failures + 1
		candidate, selErr := s.selectCandidate(stream.Context(), cpu, memoryMB)
		if selErr == nil {
			log.Printf("%s attempt=%d selected backend=%s", logPrefix, attempt, candidate.address)
			conn, down, accepted, runErr := s.dispatchHandshake(stream.Context(), candidate.address, firstReq)
			if runErr == nil {
				backendConn = conn
				backendStream = down
				acceptedResp = accepted
				log.Printf("%s attempt=%d handshake ok backend=%s", logPrefix, attempt, candidate.address)
				break
			}
			if conn != nil {
				_ = conn.Close()
			}
			log.Printf("%s attempt=%d handshake failed backend=%s retryable=%t err=%v", logPrefix, attempt, candidate.address, isRetryableDispatchError(runErr), runErr)
			if !isRetryableDispatchError(runErr) {
				return runErr
			}
			selErr = runErr
		} else {
			log.Printf("%s attempt=%d no candidate: %v", logPrefix, attempt, selErr)
		}

		if errors.Is(selErr, errNoCapacityEver) {
			log.Printf("%s reject permanently: requested cpu=%d memory_mb=%d exceeds cluster limits", logPrefix, cpu, memoryMB)
			return status.Errorf(codes.InvalidArgument, "cannot run request cpu=%d memory_mb=%d: %v", cpu, memoryMB, selErr)
		}
		failures++
		if maxRetries > 0 && failures > maxRetries {
			log.Printf("%s retries exhausted failures=%d last_err=%v", logPrefix, failures, selErr)
			return status.Errorf(codes.Unavailable, "dispatch retries exhausted after %d failures: %v", failures, selErr)
		}
		waitFor := s.computeBackoff(backoff, failures)
		log.Printf("%s attempt=%d backoff=%s before retry", logPrefix, attempt, waitFor)
		if waitErr := sleepWithContext(stream.Context(), waitFor); waitErr != nil {
			log.Printf("%s canceled during backoff: %v", logPrefix, waitErr)
			return waitErr
		}
	}
	defer backendConn.Close()

	if err := stream.Send(acceptedResp); err != nil {
		log.Printf("%s failed to send accepted response: %v", logPrefix, err)
		return err
	}
	log.Printf("%s accepted marker forwarded to client", logPrefix)

	uploadErrCh := make(chan error, 1)
	go func() {
		uploadErrCh <- forwardClientRequests(stream, backendStream)
	}()

	respFrames := 0
	for {
		resp, err := backendStream.Recv()
		if err == io.EOF {
			log.Printf("%s backend stream closed after %d response frames", logPrefix, respFrames)
			break
		}
		if err != nil {
			log.Printf("%s backend recv error after %d response frames: %v", logPrefix, respFrames, err)
			return err
		}
		respFrames++
		if sendErr := stream.Send(resp); sendErr != nil {
			log.Printf("%s failed to forward response frame=%d: %v", logPrefix, respFrames, sendErr)
			return sendErr
		}
	}
	if err := <-uploadErrCh; err != nil {
		log.Printf("%s upload forwarding error: %v", logPrefix, err)
		return err
	}
	log.Printf("%s completed successfully", logPrefix)
	return nil
}

func (s *Service) GetRuntimeStatus(ctx context.Context, req *afsletpb.GetRuntimeStatusRequest) (*afsletpb.GetRuntimeStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "afs_proxy does not implement GetRuntimeStatus")
}

func (s *Service) Status(req *afsproxypb.StatusRequest, stream afsproxypb.AfsProxy_StatusServer) error {
	includeLayerstores := true
	includeAfslets := true
	if req != nil {
		includeLayerstores = req.GetIncludeLayerstores()
		includeAfslets = req.GetIncludeAfslets()
	}

	var layerstoreCount int64
	var afsletCount int64
	var afsletReachable int64
	var totalLayers int64

	if includeLayerstores {
		services, err := s.fetchLayerstoreServices(stream.Context())
		if err != nil {
			_ = stream.Send(&afsproxypb.StatusResponse{
				Payload: &afsproxypb.StatusResponse_Error{
					Error: &afsproxypb.StatusError{
						Source:  "discovery",
						Message: err.Error(),
					},
				},
			})
		} else {
			layerstoreCount = int64(len(services))
			for _, inst := range services {
				totalLayers += int64(len(inst.GetLayers()))
				if err := stream.Send(&afsproxypb.StatusResponse{
					Payload: &afsproxypb.StatusResponse_Layerstore{Layerstore: inst},
				}); err != nil {
					return err
				}
			}
		}
	}

	if includeAfslets {
		addresses, err := resolveHostPorts(stream.Context(), s.afsletTarget)
		if err != nil {
			_ = stream.Send(&afsproxypb.StatusResponse{
				Payload: &afsproxypb.StatusResponse_Error{
					Error: &afsproxypb.StatusError{
						Source:  "afslet_resolve",
						Message: err.Error(),
					},
				},
			})
		} else {
			afsletCount = int64(len(addresses))
			for _, addr := range addresses {
				inst := &afsproxypb.AfsletInstance{Endpoint: addr}
				st, serr := s.fetchRuntimeStatus(stream.Context(), addr)
				if serr != nil {
					inst.Reachable = false
					inst.Error = serr.Error()
				} else {
					afsletReachable++
					inst.Reachable = true
					inst.RunningContainers = st.GetRunningContainers()
					inst.LimitCpuCores = st.GetLimitCpuCores()
					inst.LimitMemoryMb = st.GetLimitMemoryMb()
					inst.UsedCpuCores = st.GetUsedCpuCores()
					inst.UsedMemoryMb = st.GetUsedMemoryMb()
					inst.AvailableCpuCores = st.GetAvailableCpuCores()
					inst.AvailableMemoryMb = st.GetAvailableMemoryMb()
				}
				if err := stream.Send(&afsproxypb.StatusResponse{
					Payload: &afsproxypb.StatusResponse_Afslet{Afslet: inst},
				}); err != nil {
					return err
				}
			}
		}
	}

	return stream.Send(&afsproxypb.StatusResponse{
		Payload: &afsproxypb.StatusResponse_Summary{
			Summary: &afsproxypb.StatusSummary{
				LayerstoreInstances: layerstoreCount,
				AfsletInstances:     afsletCount,
				AfsletReachable:     afsletReachable,
				TotalLayers:         totalLayers,
			},
		},
	})
}

func (s *Service) ReconcileImageReplica(ctx context.Context, req *afsproxypb.ReconcileImageReplicaRequest) (*afsproxypb.ReconcileImageReplicaResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	image := strings.TrimSpace(req.GetImage())
	if image == "" {
		return nil, status.Error(codes.InvalidArgument, "image is required")
	}
	if req.GetReplica() < 0 {
		return nil, status.Error(codes.InvalidArgument, "replica must be >= 0")
	}

	imageKey := imageKey(
		image,
		req.GetTag(),
		valueOrDefault(req.GetPlatformOs(), defaultPlatformOS),
		valueOrDefault(req.GetPlatformArch(), defaultPlatformArch),
		req.GetPlatformVariant(),
		s.formatVersion,
	)
	resolved, err := s.resolveImage(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "resolve image: %v", err)
	}
	targetLayers := uniqueResolvedLayers(resolved.GetLayers())

	requestedReplica := req.GetReplica()
	if len(targetLayers) == 0 {
		return buildReconcileImageReplicaResponse(imageKey, requestedReplica, 0, requestedReplica == 0, nil, nil), nil
	}

	allServices, err := s.fetchDiscoveryProviders(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "query discovery candidates: %v", err)
	}
	currentReplica := minLayerReplica(targetLayers, allServices)
	if int32(currentReplica) >= requestedReplica {
		return buildReconcileImageReplicaResponse(imageKey, requestedReplica, int32(currentReplica), true, targetLayers, allServices), nil
	}

	plan := planLayerReplicaAssignments(targetLayers, int(requestedReplica), allServices)
	if len(plan) > 0 {
		if execErr := s.ensureLayerReplicaPlan(ctx, req, plan); execErr != nil {
			log.Printf("[reconcile-image] ensure plan completed with errors image=%s tag=%s err=%v", req.GetImage(), req.GetTag(), execErr)
		}
	}
	confirmedReplica, waitErr := s.waitLayerReplica(ctx, targetLayers, int(requestedReplica))
	if waitErr == nil {
		currentReplica = confirmedReplica
	}
	responseServices := allServices
	if refreshedServices, refreshErr := s.fetchDiscoveryProviders(ctx); refreshErr == nil {
		responseServices = refreshedServices
		currentReplica = minLayerReplica(targetLayers, refreshedServices)
	} else if waitErr != nil {
		currentReplica = minLayerReplica(targetLayers, allServices)
	}
	return buildReconcileImageReplicaResponse(
		imageKey,
		requestedReplica,
		int32(currentReplica),
		int32(currentReplica) >= requestedReplica,
		targetLayers,
		responseServices,
	), nil
}

func (s *Service) HandleStatusHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/status" {
		http.NotFound(w, r)
		return
	}
	includeCluster := parseBoolDefaultTrue(r.URL.Query().Get("include_cluster"))

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
		log.Printf("[proxy-status] resolve peers target=%s failed: %v", s.proxyPeersTarget, err)
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
			log.Printf("[proxy-status] peer query failed peer=%s err=%v", peer, perr)
			resp.PeerErrors++
			continue
		}
		log.Printf("[proxy-status] peer=%s node=%s local_dispatching=%d", peer, peerResp.NodeID, peerResp.LocalDispatching)
		totalByNode[peerResp.NodeID] = peerResp.LocalDispatching
	}
	for _, v := range totalByNode {
		resp.ClusterDispatching += v
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) queryPeerDispatching(ctx context.Context, client *http.Client, peerAddr string) (*localDispatchResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+peerAddr+"/status?include_cluster=false", nil)
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
		log.Printf("[proxy-select] resolve target=%s failed: %v", s.afsletTarget, err)
		return nil, err
	}
	if len(addresses) == 0 {
		log.Printf("[proxy-select] resolve target=%s returned 0 addresses", s.afsletTarget)
		return nil, errNoAfsletResolved
	}

	candidates := make([]backendCandidate, 0, len(addresses))
	maxCPU := int64(0)
	maxMem := int64(0)
	observed := 0
	for _, addr := range addresses {
		st, err := s.fetchRuntimeStatus(ctx, addr)
		if err != nil {
			log.Printf("[proxy-select] status failed backend=%s err=%v", addr, err)
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
		log.Printf("[proxy-select] no candidates requested cpu=%d memory_mb=%d resolved=%d observed=%d max_limit_cpu=%d max_limit_mem=%d", cpu, memoryMB, len(addresses), observed, maxCPU, maxMem)
		if observed > 0 && (maxCPU < cpu || maxMem < memoryMB) {
			return nil, errNoCapacityEver
		}
		return nil, errNoCapacityNow
	}
	picked := s.pickByPowerOfTwoChoices(candidates)
	log.Printf("[proxy-select] picked backend=%s requested cpu=%d memory_mb=%d candidates=%d resolved=%d observed=%d backend_used_cpu=%d/%d backend_used_mem=%d/%d",
		picked.address, cpu, memoryMB, len(candidates), len(addresses), observed,
		picked.status.GetUsedCpuCores(), picked.status.GetLimitCpuCores(),
		picked.status.GetUsedMemoryMb(), picked.status.GetLimitMemoryMb(),
	)
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

func (s *Service) fetchLayerstoreServices(ctx context.Context) ([]*afsproxypb.LayerstoreInstance, error) {
	services, err := s.fetchDiscoveryProviders(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*afsproxypb.LayerstoreInstance, 0, len(services))
	for _, svc := range services {
		layerInfos := make([]*afsproxypb.LayerInfo, 0, len(svc.GetLayerStats()))
		if len(svc.GetLayerStats()) > 0 {
			for _, ls := range svc.GetLayerStats() {
				layerInfos = append(layerInfos, &afsproxypb.LayerInfo{
					Digest:  ls.GetDigest(),
					AfsSize: ls.GetAfsSize(),
				})
			}
		} else {
			for _, digest := range svc.GetLayerDigests() {
				layerInfos = append(layerInfos, &afsproxypb.LayerInfo{Digest: digest})
			}
		}
		out = append(out, &afsproxypb.LayerstoreInstance{
			NodeId:        svc.GetNodeId(),
			Endpoint:      svc.GetEndpoint(),
			LastSeenUnix:  svc.GetLastSeenUnix(),
			CacheMaxBytes: svc.GetCacheMaxBytes(),
			Layers:        layerInfos,
			CachedImages:  append([]string(nil), svc.GetCachedImages()...),
		})
	}
	return out, nil
}

func (s *Service) resolveImage(ctx context.Context, req *afsproxypb.ReconcileImageReplicaRequest) (*discoverypb.ResolveImageResponse, error) {
	if strings.TrimSpace(s.discoveryTarget) == "" {
		return nil, fmt.Errorf("discovery target is empty")
	}
	targets, err := resolveHostPorts(ctx, s.discoveryTarget)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, target := range targets {
		dialCtx, cancel := context.WithTimeout(ctx, s.dialTimeout)
		conn, err := grpc.DialContext(dialCtx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		client := discoverypb.NewServiceDiscoveryClient(conn)
		callCtx, callCancel := context.WithTimeout(ctx, s.statusTimeout)
		resp, err := client.ResolveImage(callCtx, &discoverypb.ResolveImageRequest{
			Image:           req.GetImage(),
			Tag:             req.GetTag(),
			PlatformOs:      valueOrDefault(req.GetPlatformOs(), defaultPlatformOS),
			PlatformArch:    valueOrDefault(req.GetPlatformArch(), defaultPlatformArch),
			PlatformVariant: strings.TrimSpace(req.GetPlatformVariant()),
		})
		callCancel()
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no discovery targets available")
}

func (s *Service) fetchDiscoveryProviders(ctx context.Context) ([]*discoverypb.ServiceInstance, error) {
	if strings.TrimSpace(s.discoveryTarget) == "" {
		return nil, fmt.Errorf("discovery target is empty")
	}
	targets, err := resolveHostPorts(ctx, s.discoveryTarget)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, target := range targets {
		dialCtx, cancel := context.WithTimeout(ctx, s.dialTimeout)
		conn, err := grpc.DialContext(dialCtx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		client := discoverypb.NewServiceDiscoveryClient(conn)
		callCtx, callCancel := context.WithTimeout(ctx, s.statusTimeout)
		resp, err := client.FindProvider(callCtx, &discoverypb.FindProviderRequest{})
		callCancel()
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return resp.GetServices(), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no discovery targets available")
}

func (s *Service) ensureLayersOnLayerstore(ctx context.Context, endpoint string, req *afsproxypb.ReconcileImageReplicaRequest, layers []*discoverypb.ImageLayer) error {
	dialCtx, dialCancel := context.WithTimeout(ctx, s.dialTimeout)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	pullCtx, pullCancel := context.WithTimeout(ctx, pullImageRPCTimeout)
	defer pullCancel()
	client := layerstorepb.NewLayerStoreClient(conn)
	ensureReq := &layerstorepb.EnsureLayersRequest{
		Image:           req.GetImage(),
		Tag:             req.GetTag(),
		PlatformOs:      valueOrDefault(req.GetPlatformOs(), defaultPlatformOS),
		PlatformArch:    valueOrDefault(req.GetPlatformArch(), defaultPlatformArch),
		PlatformVariant: strings.TrimSpace(req.GetPlatformVariant()),
		Layers:          make([]*layerstorepb.Layer, 0, len(layers)),
	}
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		ensureReq.Layers = append(ensureReq.Layers, &layerstorepb.Layer{
			Digest:         layer.GetDigest(),
			MediaType:      layer.GetMediaType(),
			CompressedSize: layer.GetCompressedSize(),
		})
	}
	_, err = client.EnsureLayers(pullCtx, ensureReq)
	return err
}

func (s *Service) ensureLayerReplicaPlan(ctx context.Context, req *afsproxypb.ReconcileImageReplicaRequest, plan map[string][]*discoverypb.ImageLayer) error {
	type ensureResult struct {
		endpoint string
		err      error
	}
	results := make(chan ensureResult, len(plan))
	for endpoint, layers := range plan {
		endpoint := endpoint
		layers := append([]*discoverypb.ImageLayer(nil), layers...)
		go func() {
			results <- ensureResult{
				endpoint: endpoint,
				err:      s.ensureLayersOnLayerstore(ctx, endpoint, req, layers),
			}
		}()
	}

	var firstErr error
	for range plan {
		result := <-results
		if result.err != nil {
			log.Printf("[reconcile-image] ensure layers failed endpoint=%s err=%v", result.endpoint, result.err)
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		log.Printf("[reconcile-image] ensured layers endpoint=%s", result.endpoint)
	}
	return firstErr
}

func waitForLayerReplicaFromServices(layers []*discoverypb.ImageLayer, services []*discoverypb.ServiceInstance) int {
	return minLayerReplica(layers, services)
}

func buildReconcileImageReplicaResponse(imageKey string, requestedReplica int32, currentReplica int32, ensured bool, layers []*discoverypb.ImageLayer, services []*discoverypb.ServiceInstance) *afsproxypb.ReconcileImageReplicaResponse {
	return &afsproxypb.ReconcileImageReplicaResponse{
		ImageKey:         imageKey,
		CurrentReplica:   currentReplica,
		RequestedReplica: requestedReplica,
		Ensured:          ensured,
		Layers:           buildReconciledLayerPlacements(layers, services),
	}
}

func (s *Service) waitLayerReplica(ctx context.Context, layers []*discoverypb.ImageLayer, minReplica int) (int, error) {
	waitCtx, waitCancel := context.WithTimeout(ctx, reconcilePollTimeout)
	defer waitCancel()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	last := 0
	for {
		services, err := s.fetchDiscoveryProviders(waitCtx)
		if err == nil {
			last = waitForLayerReplicaFromServices(layers, services)
			if last >= minReplica {
				return last, nil
			}
		}
		select {
		case <-waitCtx.Done():
			if last > 0 {
				return last, nil
			}
			return 0, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func buildReconciledLayerPlacements(layers []*discoverypb.ImageLayer, services []*discoverypb.ServiceInstance) []*afsproxypb.ReconciledLayerPlacement {
	if len(layers) == 0 {
		return nil
	}

	instancesByDigest := make(map[string][]*afsproxypb.LayerstoreTarget, len(layers))
	for _, svc := range services {
		if svc == nil {
			continue
		}
		endpoint := strings.TrimSpace(svc.GetEndpoint())
		if endpoint == "" {
			continue
		}
		target := &afsproxypb.LayerstoreTarget{
			NodeId:   strings.TrimSpace(svc.GetNodeId()),
			Endpoint: endpoint,
		}
		layerSet := serviceLayerSet(svc)
		for _, layer := range layers {
			if layer == nil {
				continue
			}
			digest := strings.TrimSpace(layer.GetDigest())
			if digest == "" {
				continue
			}
			if _, ok := layerSet[digest]; ok {
				instancesByDigest[digest] = append(instancesByDigest[digest], target)
			}
		}
	}

	out := make([]*afsproxypb.ReconciledLayerPlacement, 0, len(layers))
	seen := make(map[string]struct{}, len(layers))
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		digest := strings.TrimSpace(layer.GetDigest())
		if digest == "" {
			continue
		}
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		instances := append([]*afsproxypb.LayerstoreTarget(nil), instancesByDigest[digest]...)
		sort.SliceStable(instances, func(i, j int) bool {
			if instances[i].GetNodeId() != instances[j].GetNodeId() {
				return instances[i].GetNodeId() < instances[j].GetNodeId()
			}
			return instances[i].GetEndpoint() < instances[j].GetEndpoint()
		})
		out = append(out, &afsproxypb.ReconciledLayerPlacement{
			Digest:    digest,
			Instances: instances,
		})
	}
	return out
}

func uniqueResolvedLayers(layers []*discoverypb.ImageLayer) []*discoverypb.ImageLayer {
	seen := make(map[string]struct{}, len(layers))
	out := make([]*discoverypb.ImageLayer, 0, len(layers))
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		digest := strings.TrimSpace(layer.GetDigest())
		if digest == "" {
			continue
		}
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		out = append(out, layer)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].GetCompressedSize() != out[j].GetCompressedSize() {
			return out[i].GetCompressedSize() > out[j].GetCompressedSize()
		}
		return out[i].GetDigest() < out[j].GetDigest()
	})
	return out
}

func serviceLayerSet(svc *discoverypb.ServiceInstance) map[string]struct{} {
	out := make(map[string]struct{}, len(svc.GetLayerDigests())+len(svc.GetLayerStats()))
	for _, digest := range svc.GetLayerDigests() {
		digest = strings.TrimSpace(digest)
		if digest == "" {
			continue
		}
		out[digest] = struct{}{}
	}
	for _, stat := range svc.GetLayerStats() {
		if stat == nil {
			continue
		}
		digest := strings.TrimSpace(stat.GetDigest())
		if digest == "" {
			continue
		}
		out[digest] = struct{}{}
	}
	return out
}

func minLayerReplica(layers []*discoverypb.ImageLayer, services []*discoverypb.ServiceInstance) int {
	if len(layers) == 0 {
		return 0
	}
	counts := make(map[string]int, len(layers))
	for _, svc := range services {
		if svc == nil {
			continue
		}
		layerSet := serviceLayerSet(svc)
		for _, layer := range layers {
			if layer == nil {
				continue
			}
			if _, ok := layerSet[strings.TrimSpace(layer.GetDigest())]; ok {
				counts[strings.TrimSpace(layer.GetDigest())]++
			}
		}
	}
	minReplica := -1
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		count := counts[strings.TrimSpace(layer.GetDigest())]
		if minReplica < 0 || count < minReplica {
			minReplica = count
		}
	}
	if minReplica < 0 {
		return 0
	}
	return minReplica
}

type endpointReplicaState struct {
	endpoint         string
	layers           map[string]struct{}
	targetCoverage   int
	assignedCount    int
	remainingBytes   int64
	hasCacheEstimate bool
}

func planLayerReplicaAssignments(layers []*discoverypb.ImageLayer, requestedReplica int, services []*discoverypb.ServiceInstance) map[string][]*discoverypb.ImageLayer {
	if requestedReplica <= 0 || len(layers) == 0 {
		return nil
	}

	type layerNeed struct {
		layer   *discoverypb.ImageLayer
		missing int
	}

	states := make(map[string]*endpointReplicaState, len(services))
	counts := make(map[string]int, len(layers))
	for _, svc := range services {
		if svc == nil {
			continue
		}
		endpoint := strings.TrimSpace(svc.GetEndpoint())
		if endpoint == "" {
			continue
		}
		layerSet := serviceLayerSet(svc)
		state := &endpointReplicaState{
			endpoint:       endpoint,
			layers:         layerSet,
			remainingBytes: availableCacheBytes(svc),
		}
		if svc.GetCacheMaxBytes() > 0 {
			state.hasCacheEstimate = true
		}
		for _, layer := range layers {
			if layer == nil {
				continue
			}
			digest := strings.TrimSpace(layer.GetDigest())
			if _, ok := layerSet[digest]; ok {
				counts[digest]++
				state.targetCoverage++
			}
		}
		states[endpoint] = state
	}

	needs := make([]layerNeed, 0, len(layers))
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		digest := strings.TrimSpace(layer.GetDigest())
		missing := requestedReplica - counts[digest]
		if missing > 0 {
			needs = append(needs, layerNeed{layer: layer, missing: missing})
		}
	}
	sort.SliceStable(needs, func(i, j int) bool {
		leftCount := counts[strings.TrimSpace(needs[i].layer.GetDigest())]
		rightCount := counts[strings.TrimSpace(needs[j].layer.GetDigest())]
		if leftCount != rightCount {
			return leftCount < rightCount
		}
		if needs[i].layer.GetCompressedSize() != needs[j].layer.GetCompressedSize() {
			return needs[i].layer.GetCompressedSize() > needs[j].layer.GetCompressedSize()
		}
		return needs[i].layer.GetDigest() < needs[j].layer.GetDigest()
	})

	plan := make(map[string][]*discoverypb.ImageLayer)
	for _, need := range needs {
		digest := strings.TrimSpace(need.layer.GetDigest())
		size := need.layer.GetCompressedSize()
		candidates := make([]*endpointReplicaState, 0, len(states))
		for _, state := range states {
			if _, ok := state.layers[digest]; ok {
				continue
			}
			candidates = append(candidates, state)
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			leftFit := !candidates[i].hasCacheEstimate || candidates[i].remainingBytes >= size
			rightFit := !candidates[j].hasCacheEstimate || candidates[j].remainingBytes >= size
			if leftFit != rightFit {
				return leftFit
			}
			leftScore := candidates[i].targetCoverage + candidates[i].assignedCount
			rightScore := candidates[j].targetCoverage + candidates[j].assignedCount
			if leftScore != rightScore {
				return leftScore < rightScore
			}
			if candidates[i].assignedCount != candidates[j].assignedCount {
				return candidates[i].assignedCount < candidates[j].assignedCount
			}
			if candidates[i].remainingBytes != candidates[j].remainingBytes {
				return candidates[i].remainingBytes > candidates[j].remainingBytes
			}
			return candidates[i].endpoint < candidates[j].endpoint
		})

		assignments := 0
		for _, state := range candidates {
			if assignments >= need.missing {
				break
			}
			state.layers[digest] = struct{}{}
			state.assignedCount++
			state.targetCoverage++
			if state.hasCacheEstimate && size > 0 {
				state.remainingBytes -= size
				if state.remainingBytes < 0 {
					state.remainingBytes = 0
				}
			}
			plan[state.endpoint] = append(plan[state.endpoint], need.layer)
			assignments++
		}
	}
	return plan
}

func valueOrDefault(v, d string) string {
	if s := strings.TrimSpace(v); s != "" {
		return s
	}
	return d
}

func imageKey(image, tag, platformOS, platformArch, platformVariant string, formatVersion layerformat.FormatVersion) string {
	fvStr := "v1"
	if formatVersion == layerformat.FormatV2 {
		fvStr = "v2"
	}
	return strings.Join([]string{
		strings.TrimSpace(image),
		strings.TrimSpace(tag),
		strings.TrimSpace(platformOS),
		strings.TrimSpace(platformArch),
		strings.TrimSpace(platformVariant),
		fvStr,
	}, "|")
}

func (s *Service) dispatchHandshake(ctx context.Context, backendAddr string, startReq *afsletpb.ExecuteRequest) (*grpc.ClientConn, afsletpb.Afslet_ExecuteClient, *afsletpb.ExecuteResponse, error) {
	log.Printf("[proxy-dispatch] dialing backend=%s", backendAddr)
	dialCtx, dialCancel := context.WithTimeout(ctx, s.dialTimeout)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, backendAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Printf("[proxy-dispatch] dial failed backend=%s err=%v", backendAddr, err)
		return nil, nil, nil, err
	}

	backend := afsletpb.NewAfsletClient(conn)
	backendStream, err := backend.Execute(ctx)
	if err != nil {
		log.Printf("[proxy-dispatch] open stream failed backend=%s err=%v", backendAddr, err)
		return conn, nil, nil, err
	}
	if err := backendStream.Send(startReq); err != nil {
		log.Printf("[proxy-dispatch] send start failed backend=%s err=%v", backendAddr, err)
		return conn, nil, nil, err
	}

	firstResp, err := backendStream.Recv()
	if err != nil {
		log.Printf("[proxy-dispatch] recv accepted failed backend=%s err=%v", backendAddr, err)
		return conn, nil, nil, err
	}
	acc, ok := firstResp.GetPayload().(*afsletpb.ExecuteResponse_Accepted)
	if !ok || !acc.Accepted.GetAccepted() {
		log.Printf("[proxy-dispatch] backend=%s did not accept", backendAddr)
		return conn, nil, nil, fmt.Errorf("backend %s did not return accepted marker", backendAddr)
	}
	log.Printf("[proxy-dispatch] backend=%s accepted request", backendAddr)
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

func parseBoolDefaultTrue(v string) bool {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return true
	}
	b, err := strconv.ParseBool(trimmed)
	if err == nil {
		return b
	}
	return trimmed == "1"
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
