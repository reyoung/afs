package registry

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClient_GetLayersAndDownloadLayer(t *testing.T) {
	const (
		repo      = "team/app"
		tag       = "v1"
		token     = "good-token"
		digest    = "sha256:layer1"
		blobBytes = "layer-1-content"
	)

	var baseURL string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			if got := r.URL.Query().Get("service"); got != "fake-registry" {
				t.Errorf("unexpected service: %q", got)
			}
			if got := r.URL.Query().Get("scope"); got != "repository:"+repo+":pull" {
				t.Errorf("unexpected scope: %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"` + token + `"}`))
			return

		case r.URL.Path == "/v2/"+repo+"/manifests/"+tag:
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.Header().Set("Www-Authenticate", `Bearer realm="`+baseURL+`/token",service="fake-registry"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", manifestV2)
			_, _ = w.Write([]byte(`{"schemaVersion":2,"mediaType":"` + manifestV2 + `","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":10,"digest":"sha256:cfg"},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","size":100,"digest":"` + digest + `"}]}`))
			return

		case r.URL.Path == "/v2/"+repo+"/blobs/"+digest:
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.Header().Set("Www-Authenticate", `Bearer realm="`+baseURL+`/token",service="fake-registry",scope="repository:`+repo+`:pull"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(blobBytes))
			return

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	baseURL = server.URL

	host := strings.TrimPrefix(server.URL, "https://")
	image := host + "/" + repo

	client := NewClient(server.Client())

	layers, err := client.GetLayers(context.Background(), image, tag)
	if err != nil {
		t.Fatalf("GetLayers() error = %v", err)
	}
	if len(layers) != 1 {
		t.Fatalf("unexpected layers count: %d", len(layers))
	}
	if layers[0].Digest != digest || layers[0].Size != 100 {
		t.Fatalf("unexpected layer: %+v", layers[0])
	}

	rc, err := client.DownloadLayer(context.Background(), image, tag, digest)
	if err != nil {
		t.Fatalf("DownloadLayer() error = %v", err)
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(b) != blobBytes {
		t.Fatalf("unexpected blob content: %q", string(b))
	}
}

func TestClient_LoginUsesBasicAuthForTokenEndpoint(t *testing.T) {
	const (
		repo   = "team/private"
		tag    = "v1"
		user   = "alice"
		pass   = "secret"
		token  = "token-from-auth"
		digest = "sha256:layer2"
	)

	var baseURL string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			u, p, ok := r.BasicAuth()
			if !ok || u != user || p != pass {
				t.Fatalf("expected basic auth on token endpoint, got user=%q ok=%v", u, ok)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"` + token + `"}`))
			return
		case r.URL.Path == "/v2/"+repo+"/manifests/"+tag:
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.Header().Set("Www-Authenticate", `Bearer realm="`+baseURL+`/token",service="fake-registry"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"schemaVersion":2,"mediaType":"` + manifestV2 + `","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":10,"digest":"sha256:cfg"},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","size":100,"digest":"` + digest + `"}]}`))
			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	baseURL = server.URL

	client := NewClient(server.Client())
	host := strings.TrimPrefix(server.URL, "https://")
	if err := client.Login(host, user, pass); err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	image := host + "/" + repo
	layers, err := client.GetLayers(context.Background(), image, tag)
	if err != nil {
		t.Fatalf("GetLayers() error = %v", err)
	}
	if len(layers) != 1 || layers[0].Digest != digest {
		t.Fatalf("unexpected layers: %+v", layers)
	}
}

func TestClient_LoginWithToken(t *testing.T) {
	const (
		repo      = "team/token"
		tag       = "v2"
		token     = "preset-token"
		digest    = "sha256:layer3"
		blobBytes = "blob-by-token"
	)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v2/" + repo + "/manifests/" + tag:
			_, _ = w.Write([]byte(`{"schemaVersion":2,"mediaType":"` + manifestV2 + `","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":10,"digest":"sha256:cfg"},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","size":100,"digest":"` + digest + `"}]}`))
		case "/v2/" + repo + "/blobs/" + digest:
			_, _ = w.Write([]byte(blobBytes))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewClient(server.Client())
	host := strings.TrimPrefix(server.URL, "https://")
	if err := client.LoginWithToken(host, token); err != nil {
		t.Fatalf("LoginWithToken() error = %v", err)
	}

	image := host + "/" + repo
	rc, err := client.DownloadLayer(context.Background(), image, tag, digest)
	if err != nil {
		t.Fatalf("DownloadLayer() error = %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(b) != blobBytes {
		t.Fatalf("unexpected blob content: %q", string(b))
	}
}

func TestClient_GetLayersFromManifestList_DefaultPlatform(t *testing.T) {
	const (
		repo        = "library/ubuntu"
		tag         = "latest"
		token       = "good-token"
		amdDigest   = "sha256:manifest-amd64"
		layerDigest = "sha256:layer-amd64"
	)

	var baseURL string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"` + token + `"}`))
			return
		case r.URL.Path == "/v2/"+repo+"/manifests/"+tag:
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.Header().Set("Www-Authenticate", `Bearer realm="`+baseURL+`/token",service="fake-registry"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", manifestListV2)
			_, _ = w.Write([]byte(`{"schemaVersion":2,"mediaType":"` + manifestListV2 + `","manifests":[{"mediaType":"` + manifestV2 + `","size":123,"digest":"` + amdDigest + `","platform":{"architecture":"amd64","os":"linux"}},{"mediaType":"` + manifestV2 + `","size":123,"digest":"sha256:manifest-arm64","platform":{"architecture":"arm64","os":"linux"}}]}`))
			return
		case r.URL.Path == "/v2/"+repo+"/manifests/"+amdDigest:
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.Header().Set("Www-Authenticate", `Bearer realm="`+baseURL+`/token",service="fake-registry"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", manifestV2)
			_, _ = w.Write([]byte(`{"schemaVersion":2,"mediaType":"` + manifestV2 + `","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":10,"digest":"sha256:cfg"},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","size":100,"digest":"` + layerDigest + `"}]}`))
			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	baseURL = server.URL

	host := strings.TrimPrefix(server.URL, "https://")
	image := host + "/" + repo
	client := NewClient(server.Client())

	layers, err := client.GetLayers(context.Background(), image, tag)
	if err != nil {
		t.Fatalf("GetLayers() error = %v", err)
	}
	if len(layers) != 1 || layers[0].Digest != layerDigest {
		t.Fatalf("unexpected layers: %+v", layers)
	}
}

func TestClient_LoginIsScopedByRegistryHost(t *testing.T) {
	const (
		repo = "team/app"
		tag  = "v1"
		user = "alice"
		pass = "secret"
	)

	makeServer := func() *httptest.Server {
		return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotUser, gotPass, ok := r.BasicAuth()
			if !ok || gotUser != user || gotPass != pass {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.URL.Path == "/v2/"+repo+"/manifests/"+tag {
				_, _ = w.Write([]byte(`{"schemaVersion":2,"mediaType":"` + manifestV2 + `","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":10,"digest":"sha256:cfg"},"layers":[]}`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
	}

	serverA := makeServer()
	defer serverA.Close()
	serverB := makeServer()
	defer serverB.Close()

	client := NewClient(&http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	})
	hostA := strings.TrimPrefix(serverA.URL, "https://")
	hostB := strings.TrimPrefix(serverB.URL, "https://")

	if err := client.Login(hostA, user, pass); err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	_, err := client.GetManifest(context.Background(), hostA+"/"+repo, tag)
	if err != nil {
		t.Fatalf("GetManifest(hostA) error = %v", err)
	}

	_, err = client.GetManifest(context.Background(), hostB+"/"+repo, tag)
	if err == nil {
		t.Fatal("expected hostB request to fail without host-specific credentials")
	}
}
