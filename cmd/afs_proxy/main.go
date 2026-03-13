package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/reyoung/afs/pkg/afsletpb"
	"github.com/reyoung/afs/pkg/afsproxy"
	"github.com/reyoung/afs/pkg/afsproxypb"
	"github.com/reyoung/afs/pkg/debughttp"
)

func main() {
	var (
		grpcListen      string
		httpListen      string
		afsletTarget    string
		proxyPeers      string
		nodeID          string
		dialTimeout     time.Duration
		statusTimeout   time.Duration
		defaultBackoff  time.Duration
		httpPeerTimeout time.Duration
		gracefulTimeout time.Duration
		discoveryTarget string
		pprofListen     string
		formatVersion   int
	)

	flag.StringVar(&grpcListen, "listen", ":62051", "gRPC listen address")
	flag.StringVar(&httpListen, "http-listen", ":62052", "HTTP listen address for dispatching status")
	flag.StringVar(&afsletTarget, "afslet-target", "afslet:61051", "afslet DNS target host:port (A records are re-resolved every attempt)")
	flag.StringVar(&proxyPeers, "proxy-peers-target", "", "afs_proxy DNS target host:port for cluster dispatching status aggregation")
	flag.StringVar(&nodeID, "node-id", "", "node id for cluster status dedup")
	flag.DurationVar(&dialTimeout, "dial-timeout", 3*time.Second, "backend dial timeout")
	flag.DurationVar(&statusTimeout, "status-timeout", 2*time.Second, "backend status RPC timeout")
	flag.DurationVar(&defaultBackoff, "dispatch-backoff", 50*time.Millisecond, "default dispatch retry backoff")
	flag.DurationVar(&httpPeerTimeout, "peer-http-timeout", 2*time.Second, "peer query timeout for cluster dispatching status")
	flag.DurationVar(&gracefulTimeout, "graceful-timeout", 10*time.Second, "graceful shutdown timeout")
	flag.StringVar(&discoveryTarget, "discovery-target", "", "discovery DNS target host:port for layerstore status query")
	flag.StringVar(&pprofListen, "pprof-listen", "", "optional HTTP listen address for pprof; use the same value as -http-listen to expose pprof on the main HTTP server")
	flag.IntVar(&formatVersion, "format-version", 2, "AFS layer format version (1=AFSLYR01, 2=AFSLYR02); default is 2")
	flag.Parse()

	svc := afsproxy.NewService(afsproxy.Config{
		AfsletTarget:      afsletTarget,
		ProxyPeersTarget:  proxyPeers,
		NodeID:            nodeID,
		FormatVersion:     formatVersion,
		DialTimeout:       dialTimeout,
		StatusTimeout:     statusTimeout,
		DefaultBackoff:    defaultBackoff,
		HTTPClientTimeout: httpPeerTimeout,
		DiscoveryTarget:   discoveryTarget,
	})

	grpcLis, err := net.Listen("tcp", grpcListen)
	if err != nil {
		log.Fatalf("listen grpc %s: %v", grpcListen, err)
	}
	httpLis, err := net.Listen("tcp", httpListen)
	if err != nil {
		log.Fatalf("listen http %s: %v", httpListen, err)
	}

	grpcServer := grpc.NewServer()
	afsletpb.RegisterAfsletServer(grpcServer, svc)
	afsproxypb.RegisterAfsProxyServer(grpcServer, svc)
	reflection.Register(grpcServer)

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/status", svc.HandleStatusHTTP)
	// Backward compatibility for old clients.
	httpMux.HandleFunc("/dispatching", svc.HandleStatusHTTP)
	if pprofListen != "" && pprofListen == httpListen {
		debughttp.RegisterPprof(httpMux)
	}
	httpServer := &http.Server{Handler: httpMux}
	shutdownPprof := func(context.Context) error { return nil }
	if pprofListen != "" && pprofListen != httpListen {
		if stopper := debughttp.StartPprofServer("afs_proxy", pprofListen); stopper != nil {
			shutdownPprof = stopper
		}
	}

	go func() {
		log.Printf("afs_proxy gRPC listening on %s (afslet-target=%s)", grpcListen, afsletTarget)
		if err := grpcServer.Serve(grpcLis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()
	go func() {
		log.Printf("afs_proxy HTTP listening on %s", httpListen)
		if err := httpServer.Serve(httpLis); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), gracefulTimeout)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		grpcServer.Stop()
	}
	_ = shutdownPprof(ctx)
}
