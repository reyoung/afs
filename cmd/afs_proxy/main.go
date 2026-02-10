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
	flag.Parse()

	svc := afsproxy.NewService(afsproxy.Config{
		AfsletTarget:      afsletTarget,
		ProxyPeersTarget:  proxyPeers,
		NodeID:            nodeID,
		DialTimeout:       dialTimeout,
		StatusTimeout:     statusTimeout,
		DefaultBackoff:    defaultBackoff,
		HTTPClientTimeout: httpPeerTimeout,
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
	reflection.Register(grpcServer)

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/dispatching", svc.HandleDispatchingHTTP)
	httpServer := &http.Server{Handler: httpMux}

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
}
