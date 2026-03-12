package debughttp

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

func RegisterPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func StartPprofServer(component string, listenAddr string) func(context.Context) error {
	if listenAddr == "" {
		return nil
	}

	mux := http.NewServeMux()
	RegisterPprof(mux)
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		ln, err := net.Listen("tcp", listenAddr)
		if err != nil {
			log.Fatalf("%s pprof listen %s: %v", component, listenAddr, err)
		}
		log.Printf("%s pprof listening on %s", component, listenAddr)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("%s pprof serve: %v", component, err)
		}
	}()

	return server.Shutdown
}
