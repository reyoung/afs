package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/reyoung/afs/pkg/discoverypb"
	"github.com/reyoung/afs/pkg/layerstore"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

const heartbeatInterval = 10 * time.Second

func main() {
	var (
		listenAddr         string
		cacheDir           string
		nodeID             string
		authHost           string
		authUser           string
		authPass           string
		authToken          string
		authPairs          multiStringFlag
		basicPairs         multiStringFlag
		discoveryEndpoints multiStringFlag
	)

	flag.StringVar(&listenAddr, "listen", "", "gRPC listen endpoint with IP, e.g. 10.0.0.12:50051")
	flag.StringVar(&cacheDir, "cache-dir", ".cache/layerstore", "cache root directory")
	flag.StringVar(&nodeID, "node-id", "", "unique node identifier for discovery affinity")
	flag.StringVar(&authHost, "auth-registry", "", "registry host for startup auth, e.g. registry-1.docker.io")
	flag.StringVar(&authUser, "auth-username", "", "registry username")
	flag.StringVar(&authPass, "auth-password", "", "registry password")
	flag.StringVar(&authToken, "auth-token", "", "registry bearer token")
	flag.Var(&authPairs, "auth-registry-token", "repeatable registry token pair, format <registry>=<token>")
	flag.Var(&basicPairs, "auth-registry-basic", "repeatable registry basic auth, format <registry>=<username>[:<password>]")
	flag.Var(&discoveryEndpoints, "discovery-endpoint", "repeatable discovery gRPC endpoint, e.g. 10.0.0.2:60051")
	flag.Parse()

	if err := validateListenEndpoint(listenAddr); err != nil {
		log.Fatalf("invalid -listen: %v", err)
	}
	if strings.TrimSpace(nodeID) == "" {
		log.Fatal("-node-id is required")
	}

	var authConfigs []layerstore.RegistryAuthConfig
	for _, pair := range authPairs {
		host, token, err := parseRegistryTokenPair(pair)
		if err != nil {
			log.Fatalf("invalid -auth-registry-token %q: %v", pair, err)
		}
		authConfigs = append(authConfigs, layerstore.RegistryAuthConfig{
			RegistryHost: host,
			BearerToken:  token,
		})
	}
	for _, pair := range basicPairs {
		host, username, password, err := parseRegistryBasicPair(pair)
		if err != nil {
			log.Fatalf("invalid -auth-registry-basic %q: %v", pair, err)
		}
		authConfigs = append(authConfigs, layerstore.RegistryAuthConfig{
			RegistryHost: host,
			Username:     username,
			Password:     password,
		})
	}
	if authHost != "" || authUser != "" || authPass != "" || authToken != "" {
		authConfigs = append(authConfigs, layerstore.RegistryAuthConfig{
			RegistryHost: authHost,
			Username:     authUser,
			Password:     authPass,
			BearerToken:  authToken,
		})
	}
	log.Printf("starting layerstore: node-id=%s listen=%s cache-dir=%s auth-configs=%d discovery-endpoints=%d", nodeID, listenAddr, cacheDir, len(authConfigs), len(discoveryEndpoints))

	svc, err := layerstore.NewService(cacheDir, authConfigs)
	if err != nil {
		log.Fatalf("init service: %v", err)
	}
	var pendingMu sync.RWMutex
	pending := make(map[string]struct{})
	svc.SetPullImageReporter(func(imgKey string, pulling bool) {
		pendingMu.Lock()
		defer pendingMu.Unlock()
		if pulling {
			pending[imgKey] = struct{}{}
			return
		}
		delete(pending, imgKey)
	})

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	grpcServer := grpc.NewServer()
	layerstorepb.RegisterLayerStoreServer(grpcServer, svc)
	reflection.Register(grpcServer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, endpoint := range discoveryEndpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		go registerHeartbeatLoop(ctx, endpoint, nodeID, listenAddr, cacheDir, func() []string {
			pendingMu.RLock()
			defer pendingMu.RUnlock()
			out := make([]string, 0, len(pending))
			for k := range pending {
				out = append(out, k)
			}
			return out
		})
	}

	go func() {
		log.Printf("layerstore gRPC listening on %s, cache-dir=%s", listenAddr, cacheDir)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Fatalf("grpc serve: %v", serveErr)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	log.Printf("shutting down layerstore gRPC server")
	grpcServer.GracefulStop()
}

func registerHeartbeatLoop(ctx context.Context, discoveryAddr, nodeID, endpoint, cacheDir string, pendingImages func() []string) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	send := func() {
		layers, err := scanCachedLayerDigests(cacheDir)
		if err != nil {
			log.Printf("heartbeat scan cache failed: discovery=%s err=%v", discoveryAddr, err)
			layers = nil
		}
		images, err := scanCachedImageKeys(cacheDir)
		if err != nil {
			log.Printf("heartbeat scan image metadata failed: discovery=%s err=%v", discoveryAddr, err)
			images = nil
		}
		images = mergeStringSets(images, pendingImages())
		targets, err := expandHeartbeatTargets(ctx, discoveryAddr)
		if err != nil {
			log.Printf("heartbeat resolve failed: discovery=%s err=%v", discoveryAddr, err)
			return
		}
		for _, target := range targets {
			if err := sendHeartbeatOnce(ctx, target, nodeID, endpoint, layers, images); err != nil {
				log.Printf("heartbeat failed: discovery=%s target=%s err=%v", discoveryAddr, target, err)
				continue
			}
			log.Printf("heartbeat sent: discovery=%s target=%s endpoint=%s layers=%d images=%d", discoveryAddr, target, endpoint, len(layers), len(images))
		}
	}

	send()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

func sendHeartbeatOnce(ctx context.Context, target, nodeID, endpoint string, layers []string, images []string) error {
	hbCtx, hbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer hbCancel()

	conn, err := grpc.DialContext(hbCtx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := discoverypb.NewServiceDiscoveryClient(conn)
	_, err = client.Heartbeat(hbCtx, &discoverypb.HeartbeatRequest{
		NodeId:       nodeID,
		Endpoint:     endpoint,
		LayerDigests: layers,
		CachedImages: images,
	})
	return err
}

func expandHeartbeatTargets(ctx context.Context, discoveryAddr string) ([]string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(discoveryAddr))
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		return []string{discoveryAddr}, nil
	}

	lookupCtx, lookupCancel := context.WithTimeout(ctx, 3*time.Second)
	defer lookupCancel()

	ipAddrs, err := net.DefaultResolver.LookupIPAddr(lookupCtx, host)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(ipAddrs))
	out := make([]string, 0, len(ipAddrs))
	for _, ipAddr := range ipAddrs {
		ip := strings.TrimSpace(ipAddr.IP.String())
		if ip == "" {
			continue
		}
		target := net.JoinHostPort(ip, port)
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no resolved addresses for host %q (discovery addr %q)", host, discoveryAddr)
	}
	sort.Strings(out)
	return out, nil
}

func scanCachedLayerDigests(cacheDir string) ([]string, error) {
	layersDir := filepath.Join(cacheDir, "layers")
	entries, err := os.ReadDir(layersDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, algoEnt := range entries {
		if !algoEnt.IsDir() {
			continue
		}
		algo := strings.TrimSpace(algoEnt.Name())
		if algo == "" {
			continue
		}
		dir := filepath.Join(layersDir, algo)
		files, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			name := f.Name()
			if !strings.HasSuffix(name, ".afslyr") {
				continue
			}
			hex := strings.TrimSuffix(name, ".afslyr")
			if hex == "" {
				continue
			}
			out = append(out, strings.ToLower(algo)+":"+hex)
		}
	}
	return out, nil
}

type cachedMeta struct {
	Image           string `json:"image"`
	Tag             string `json:"tag"`
	PlatformOS      string `json:"platform_os"`
	PlatformArch    string `json:"platform_arch"`
	PlatformVariant string `json:"platform_variant"`
}

func scanCachedImageKeys(cacheDir string) ([]string, error) {
	metadataDir := filepath.Join(cacheDir, "metadata")
	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		path := filepath.Join(metadataDir, ent.Name())
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var m cachedMeta
		if err := json.Unmarshal(b, &m); err != nil {
			continue
		}
		if strings.TrimSpace(m.Image) == "" {
			continue
		}
		key := imageKey(m.Image, m.Tag, m.PlatformOS, m.PlatformArch, m.PlatformVariant)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out, nil
}

func imageKey(image, tag, platformOS, platformArch, platformVariant string) string {
	return strings.Join([]string{
		strings.TrimSpace(image),
		strings.TrimSpace(tag),
		strings.TrimSpace(platformOS),
		strings.TrimSpace(platformArch),
		strings.TrimSpace(platformVariant),
	}, "|")
}

func mergeStringSets(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, v := range a {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, v := range b {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func validateListenEndpoint(v string) error {
	host, port, err := net.SplitHostPort(strings.TrimSpace(v))
	if err != nil {
		return err
	}
	if net.ParseIP(host) == nil {
		return fmt.Errorf("host must be an IP address, got %q", host)
	}
	if strings.TrimSpace(port) == "" {
		return fmt.Errorf("port is required")
	}
	return nil
}

type multiStringFlag []string

func (m *multiStringFlag) String() string {
	if m == nil {
		return ""
	}
	return strings.Join(*m, ",")
}

func (m *multiStringFlag) Set(v string) error {
	*m = append(*m, strings.TrimSpace(v))
	return nil
}

func parseRegistryTokenPair(v string) (registryHost string, token string, err error) {
	parts := strings.SplitN(strings.TrimSpace(v), "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected <registry>=<token>")
	}
	host := strings.TrimSpace(parts[0])
	tok := strings.TrimSpace(parts[1])
	if host == "" || tok == "" {
		return "", "", fmt.Errorf("registry and token must be non-empty")
	}
	return host, tok, nil
}

func parseRegistryBasicPair(v string) (registryHost string, username string, password string, err error) {
	parts := strings.SplitN(strings.TrimSpace(v), "=", 2)
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("expected <registry>=<username>[:<password>]")
	}
	host := strings.TrimSpace(parts[0])
	cred := strings.TrimSpace(parts[1])
	if host == "" || cred == "" {
		return "", "", "", fmt.Errorf("registry and username must be non-empty")
	}
	userPass := strings.SplitN(cred, ":", 2)
	user := strings.TrimSpace(userPass[0])
	if user == "" {
		return "", "", "", fmt.Errorf("username must be non-empty")
	}
	pass := ""
	if len(userPass) == 2 {
		pass = userPass[1]
	}
	return host, user, pass, nil
}
