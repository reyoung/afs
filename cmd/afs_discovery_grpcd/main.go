package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/reyoung/afs/pkg/discovery"
	"github.com/reyoung/afs/pkg/discoverypb"
)

func main() {
	var (
		listenAddr  string
		authHost    string
		authUser    string
		authPass    string
		authToken   string
		authPairs   multiStringFlag
		basicPairs  multiStringFlag
		mirrorPairs multiStringFlag
	)
	flag.StringVar(&listenAddr, "listen", ":60051", "gRPC listen address")
	flag.StringVar(&authHost, "auth-registry", "", "registry host for startup auth, e.g. registry-1.docker.io")
	flag.StringVar(&authUser, "auth-username", "", "registry username")
	flag.StringVar(&authPass, "auth-password", "", "registry password")
	flag.StringVar(&authToken, "auth-token", "", "registry bearer token")
	flag.Var(&authPairs, "auth-registry-token", "repeatable registry token pair, format <registry>=<token>")
	flag.Var(&basicPairs, "auth-registry-basic", "repeatable registry basic auth, format <registry>=<username>[:<password>]")
	flag.Var(&mirrorPairs, "registry-mirror", "repeatable registry mirror mapping, format <registry>=<mirror>[,<mirror>...]")
	flag.Parse()

	var authConfigs []discovery.RegistryAuthConfig
	for _, pair := range authPairs {
		host, token, err := parseRegistryTokenPair(pair)
		if err != nil {
			log.Fatalf("invalid -auth-registry-token %q: %v", pair, err)
		}
		authConfigs = append(authConfigs, discovery.RegistryAuthConfig{
			RegistryHost: host,
			BearerToken:  token,
		})
	}
	for _, pair := range basicPairs {
		host, username, password, err := parseRegistryBasicPair(pair)
		if err != nil {
			log.Fatalf("invalid -auth-registry-basic %q: %v", pair, err)
		}
		authConfigs = append(authConfigs, discovery.RegistryAuthConfig{
			RegistryHost: host,
			Username:     username,
			Password:     password,
		})
	}
	if authHost != "" || authUser != "" || authPass != "" || authToken != "" {
		authConfigs = append(authConfigs, discovery.RegistryAuthConfig{
			RegistryHost: authHost,
			Username:     authUser,
			Password:     authPass,
			BearerToken:  authToken,
		})
	}
	registryMirrors := make(map[string][]string)
	for _, pair := range mirrorPairs {
		host, mirrors, err := parseRegistryMirrorPair(pair)
		if err != nil {
			log.Fatalf("invalid -registry-mirror %q: %v", pair, err)
		}
		registryMirrors[host] = append(registryMirrors[host], mirrors...)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	svc := discovery.NewService()
	if err := svc.SetRegistryAuthConfigs(authConfigs); err != nil {
		log.Fatalf("configure registry auth: %v", err)
	}
	if len(registryMirrors) > 0 {
		svc.SetRegistryMirrors(registryMirrors)
	}
	server := grpc.NewServer()
	discoverypb.RegisterServiceDiscoveryServer(server, svc)
	reflection.Register(server)

	go func() {
		log.Printf("discovery gRPC listening on %s", listenAddr)
		if err := server.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down discovery gRPC server")
	server.GracefulStop()
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

func parseRegistryMirrorPair(v string) (registryHost string, mirrors []string, err error) {
	parts := strings.SplitN(strings.TrimSpace(v), "=", 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("expected <registry>=<mirror>[,<mirror>...]")
	}
	host := strings.TrimSpace(parts[0])
	rawMirrors := strings.TrimSpace(parts[1])
	if host == "" || rawMirrors == "" {
		return "", nil, fmt.Errorf("registry and mirror list must be non-empty")
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, 4)
	for _, m := range strings.Split(rawMirrors, ",") {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	if len(out) == 0 {
		return "", nil, fmt.Errorf("at least one mirror is required")
	}
	return host, out, nil
}
