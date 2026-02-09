package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/reyoung/afs/pkg/discovery"
	"github.com/reyoung/afs/pkg/discoverypb"
)

func main() {
	var listenAddr string
	flag.StringVar(&listenAddr, "listen", ":60051", "gRPC listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	svc := discovery.NewService()
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
