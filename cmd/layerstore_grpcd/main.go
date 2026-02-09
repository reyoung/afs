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

	"github.com/reyoung/afs/pkg/layerstore"
	"github.com/reyoung/afs/pkg/layerstorepb"
)

func main() {
	var (
		listenAddr string
		cacheDir   string
		authHost   string
		authUser   string
		authPass   string
		authToken  string
		authPairs  multiStringFlag
		basicPairs multiStringFlag
	)

	flag.StringVar(&listenAddr, "listen", ":50051", "gRPC listen address")
	flag.StringVar(&cacheDir, "cache-dir", ".cache/layerstore", "cache root directory")
	flag.StringVar(&authHost, "auth-registry", "", "registry host for startup auth, e.g. registry-1.docker.io")
	flag.StringVar(&authUser, "auth-username", "", "registry username")
	flag.StringVar(&authPass, "auth-password", "", "registry password")
	flag.StringVar(&authToken, "auth-token", "", "registry bearer token")
	flag.Var(&authPairs, "auth-registry-token", "repeatable registry token pair, format <registry>=<token>")
	flag.Var(&basicPairs, "auth-registry-basic", "repeatable registry basic auth, format <registry>=<username>[:<password>]")
	flag.Parse()

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
	log.Printf("starting layerstore: listen=%s cache-dir=%s auth-configs=%d", listenAddr, cacheDir, len(authConfigs))

	svc, err := layerstore.NewService(cacheDir, authConfigs)
	if err != nil {
		log.Fatalf("init service: %v", err)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	grpcServer := grpc.NewServer()
	layerstorepb.RegisterLayerStoreServer(grpcServer, svc)
	reflection.Register(grpcServer)

	go func() {
		log.Printf("layerstore gRPC listening on %s, cache-dir=%s", listenAddr, cacheDir)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Fatalf("grpc serve: %v", serveErr)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down layerstore gRPC server")
	grpcServer.GracefulStop()
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
