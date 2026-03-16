package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/reyoung/afs/pkg/afsproxy"
)

func TestNewHTTPMux(t *testing.T) {
	t.Parallel()

	mux := newHTTPMux(afsproxy.NewService(afsproxy.Config{}))

	statusReq := httptest.NewRequest(http.MethodGet, "http://example/status?include_cluster=false", nil)
	statusRec := httptest.NewRecorder()
	mux.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status endpoint code=%d, want %d", statusRec.Code, http.StatusOK)
	}

	pprofReq := httptest.NewRequest(http.MethodGet, "http://example/debug/pprof/", nil)
	pprofRec := httptest.NewRecorder()
	mux.ServeHTTP(pprofRec, pprofReq)
	if pprofRec.Code != http.StatusOK {
		t.Fatalf("pprof endpoint code=%d, want %d", pprofRec.Code, http.StatusOK)
	}

	legacyReq := httptest.NewRequest(http.MethodGet, "http://example/dispatching", nil)
	legacyRec := httptest.NewRecorder()
	mux.ServeHTTP(legacyRec, legacyReq)
	if legacyRec.Code != http.StatusNotFound {
		t.Fatalf("legacy endpoint code=%d, want %d", legacyRec.Code, http.StatusNotFound)
	}
}
