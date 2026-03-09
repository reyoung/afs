package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	manifestV2       = "application/vnd.docker.distribution.manifest.v2+json"
	manifestOCI      = "application/vnd.oci.image.manifest.v1+json"
	manifestListV2   = "application/vnd.docker.distribution.manifest.list.v2+json"
	manifestIndexOCI = "application/vnd.oci.image.index.v1+json"

	defaultPlatformOS   = "linux"
	defaultPlatformArch = "amd64"
)

// Client provides APIs to read image metadata and blobs from a docker registry.
type Client struct {
	httpClient *http.Client
	userAgent  string

	mu             sync.RWMutex
	authByRegistry map[string]authConfig
	mirrorsByHost  map[string][]string
}

type authConfig struct {
	username    string
	password    string
	bearerToken string
}

// NewClient creates a registry client. If httpClient is nil, http.DefaultClient is used.
func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		httpClient:     httpClient,
		userAgent:      "afs-registry-client/1.0",
		authByRegistry: make(map[string]authConfig),
		mirrorsByHost:  make(map[string][]string),
	}
}

// SetRegistryMirrors configures pull mirrors for one registry host.
// Mirrors are tried in order; the original registry is used as fallback.
func (c *Client) SetRegistryMirrors(registry string, mirrors []string) error {
	registry = strings.TrimSpace(registry)
	if registry == "" {
		return fmt.Errorf("registry must not be empty")
	}
	cleaned := make([]string, 0, len(mirrors))
	seen := make(map[string]struct{}, len(mirrors))
	for _, m := range mirrors {
		h, err := normalizeMirrorHost(m)
		if err != nil {
			return fmt.Errorf("invalid mirror %q for registry %s: %w", m, registry, err)
		}
		if h == "" || h == registry {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		cleaned = append(cleaned, h)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(cleaned) == 0 {
		delete(c.mirrorsByHost, registry)
		return nil
	}
	c.mirrorsByHost[registry] = cleaned
	return nil
}

// Login configures client to use basic auth credentials for a registry host.
// When the registry returns a bearer challenge, the same credentials are used
// to request token from the auth server.
func (c *Client) Login(registry, username, password string) error {
	if strings.TrimSpace(registry) == "" {
		return fmt.Errorf("registry must not be empty")
	}
	if strings.TrimSpace(username) == "" {
		return fmt.Errorf("username must not be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.authByRegistry[registry] = authConfig{
		username: username,
		password: password,
	}
	return nil
}

// LoginWithToken configures client to send bearer token for a registry host.
func (c *Client) LoginWithToken(registry, token string) error {
	if strings.TrimSpace(registry) == "" {
		return fmt.Errorf("registry must not be empty")
	}
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("token must not be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.authByRegistry[registry] = authConfig{
		bearerToken: token,
	}
	return nil
}

// Logout clears credentials/token for the given registry host.
func (c *Client) Logout(registry string) {
	if strings.TrimSpace(registry) == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.authByRegistry, registry)
}

// LogoutAll clears credentials/tokens for all registries.
func (c *Client) LogoutAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.authByRegistry = make(map[string]authConfig)
}

// GetManifest fetches image manifest by image + tag.
func (c *Client) GetManifest(ctx context.Context, image string, tag string) (*Manifest, error) {
	return c.GetManifestForPlatform(ctx, image, tag, defaultPlatformOS, defaultPlatformArch, "")
}

// GetManifestForPlatform fetches the image manifest for a specific os/arch.
func (c *Client) GetManifestForPlatform(ctx context.Context, image string, tag string, os string, arch string, variant string) (*Manifest, error) {
	ref, err := ParseImageReference(image, tag)
	if err != nil {
		return nil, err
	}
	return c.getManifestForReference(ctx, ref, ref.Reference, os, arch, variant)
}

func (c *Client) getManifestForReference(ctx context.Context, ref ImageReference, manifestRef string, os string, arch string, variant string) (*Manifest, error) {
	hosts := c.requestHosts(ref.Registry)
	var lastErr error
	for i, host := range hosts {
		u := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, ref.Repository, manifestRef)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", strings.Join([]string{manifestOCI, manifestV2, manifestIndexOCI, manifestListV2}, ", "))

		resp, err := c.doWithAuth(ctx, req, host, ref.Repository)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
			_ = resp.Body.Close()
			reqErr := fmt.Errorf("manifest request failed host=%s status=%d body=%q", host, resp.StatusCode, strings.TrimSpace(string(b)))
			lastErr = reqErr
			if i < len(hosts)-1 && shouldFallbackToNextHost(resp.StatusCode) {
				continue
			}
			return nil, reqErr
		}

		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read manifest from host=%s: %w", host, err)
			continue
		}

		mediaType := normalizeMediaType(resp.Header.Get("Content-Type"))
		var probe struct {
			MediaType string `json:"mediaType"`
		}
		if err := json.Unmarshal(body, &probe); err != nil {
			return nil, fmt.Errorf("decode manifest metadata: %w", err)
		}
		if mediaType == "" || mediaType == "application/json" || mediaType == "text/plain" {
			mediaType = probe.MediaType
		}

		switch mediaType {
		case manifestOCI, manifestV2:
			var m Manifest
			if err := json.Unmarshal(body, &m); err != nil {
				return nil, fmt.Errorf("decode manifest: %w", err)
			}
			return &m, nil
		case manifestIndexOCI, manifestListV2:
			var ml ManifestList
			if err := json.Unmarshal(body, &ml); err != nil {
				return nil, fmt.Errorf("decode manifest list: %w", err)
			}
			entry, err := chooseManifestEntry(ml.Manifests, os, arch, variant)
			if err != nil {
				return nil, err
			}
			return c.getManifestForReference(ctx, ref, entry.Digest, os, arch, variant)
		default:
			// Fallback for registries that omit mediaType but still return single manifest.
			var m Manifest
			if err := json.Unmarshal(body, &m); err == nil && len(m.Layers) > 0 {
				return &m, nil
			}
			return nil, fmt.Errorf("unsupported manifest media type: %q", mediaType)
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("manifest request failed: no available hosts for %s", ref.Registry)
}

func normalizeMediaType(v string) string {
	if i := strings.IndexByte(v, ';'); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v)
}

func chooseManifestEntry(entries []ManifestListEntry, os string, arch string, variant string) (ManifestListEntry, error) {
	if len(entries) == 0 {
		return ManifestListEntry{}, fmt.Errorf("manifest list contains no entries")
	}
	for _, e := range entries {
		if e.Platform.OS == os && e.Platform.Architecture == arch {
			if variant == "" || e.Platform.Variant == variant {
				return e, nil
			}
		}
	}
	if variant == "" {
		for _, e := range entries {
			if e.Platform.OS == os && e.Platform.Architecture == arch {
				return e, nil
			}
		}
	}
	return ManifestListEntry{}, fmt.Errorf("no manifest for platform %s/%s variant=%q", os, arch, variant)
}

// GetLayers returns flattened layer metadata for an image.
func (c *Client) GetLayers(ctx context.Context, image string, tag string) ([]Layer, error) {
	return c.GetLayersForPlatform(ctx, image, tag, defaultPlatformOS, defaultPlatformArch, "")
}

// GetLayersForPlatform returns layer metadata for a specific os/arch.
func (c *Client) GetLayersForPlatform(ctx context.Context, image string, tag string, os string, arch string, variant string) ([]Layer, error) {
	manifest, err := c.GetManifestForPlatform(ctx, image, tag, os, arch, variant)
	if err != nil {
		return nil, err
	}
	layers := make([]Layer, 0, len(manifest.Layers))
	for _, d := range manifest.Layers {
		layers = append(layers, Layer{Digest: d.Digest, MediaType: d.MediaType, Size: d.Size})
	}
	return layers, nil
}

// DownloadLayer downloads a layer blob by digest.
func (c *Client) DownloadLayer(ctx context.Context, image string, tag string, digest string) (io.ReadCloser, error) {
	if strings.TrimSpace(digest) == "" {
		return nil, fmt.Errorf("digest must not be empty")
	}
	ref, err := ParseImageReference(image, tag)
	if err != nil {
		return nil, err
	}

	hosts := c.requestHosts(ref.Registry)
	var lastErr error
	for i, host := range hosts {
		u := fmt.Sprintf("https://%s/v2/%s/blobs/%s", host, ref.Repository, digest)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}

		resp, err := c.doWithAuth(ctx, req, host, ref.Repository)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return resp.Body, nil
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		_ = resp.Body.Close()
		reqErr := fmt.Errorf("download layer failed host=%s status=%d body=%q", host, resp.StatusCode, strings.TrimSpace(string(b)))
		lastErr = reqErr
		if i < len(hosts)-1 && shouldFallbackToNextHost(resp.StatusCode) {
			continue
		}
		return nil, reqErr
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("download layer failed: no available hosts for %s", ref.Registry)
}

func (c *Client) doWithAuth(ctx context.Context, req *http.Request, registry, repository string) (*http.Response, error) {
	cfg := c.authSnapshot(registry)
	req.Header.Set("User-Agent", c.userAgent)
	applyAuthHeader(req, cfg)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	challenge := resp.Header.Get("Www-Authenticate")
	resp.Body.Close()
	realm, service, scope, err := parseBearerChallenge(challenge)
	if err != nil {
		return nil, fmt.Errorf("registry auth challenge: %w", err)
	}
	if scope == "" {
		scope = "repository:" + repository + ":pull"
	}

	token, err := c.fetchToken(ctx, realm, service, scope, cfg.username, cfg.password)
	if err != nil {
		return nil, err
	}

	retry := req.Clone(ctx)
	copyHeaders(retry.Header, req.Header)
	retry.Header.Set("Authorization", "Bearer "+token)
	return c.httpClient.Do(retry)
}

func (c *Client) fetchToken(ctx context.Context, realm, service, scope, username, password string) (string, error) {
	if realm == "" {
		return "", fmt.Errorf("missing auth realm")
	}
	u, err := url.Parse(realm)
	if err != nil {
		return "", fmt.Errorf("invalid auth realm: %w", err)
	}
	q := u.Query()
	if service != "" {
		q.Set("service", service)
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", c.userAgent)
	if username != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return "", fmt.Errorf("token request failed: status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var data struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	token := data.Token
	if token == "" {
		token = data.AccessToken
	}
	if token == "" {
		return "", fmt.Errorf("token response missing token field")
	}
	return token, nil
}

func parseBearerChallenge(v string) (realm, service, scope string, err error) {
	if !strings.HasPrefix(strings.ToLower(v), "bearer ") {
		return "", "", "", fmt.Errorf("unsupported challenge: %q", v)
	}
	fields := strings.Split(v[len("Bearer "):], ",")
	for _, f := range fields {
		part := strings.TrimSpace(f)
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		val := strings.Trim(strings.TrimSpace(kv[1]), "\"")
		switch key {
		case "realm":
			realm = val
		case "service":
			service = val
		case "scope":
			scope = val
		}
	}
	if realm == "" {
		return "", "", "", fmt.Errorf("missing realm in challenge: %q", v)
	}
	return realm, service, scope, nil
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func (c *Client) authSnapshot(registry string) authConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authByRegistry[registry]
}

func (c *Client) mirrorSnapshot(registry string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := c.mirrorsByHost[registry]
	if len(out) == 0 {
		return nil
	}
	cp := make([]string, len(out))
	copy(cp, out)
	return cp
}

func (c *Client) requestHosts(registry string) []string {
	registry = strings.TrimSpace(registry)
	mirrors := c.mirrorSnapshot(registry)
	out := make([]string, 0, len(mirrors)+1)
	seen := make(map[string]struct{}, len(mirrors)+1)
	for _, host := range mirrors {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	if registry != "" {
		if _, ok := seen[registry]; !ok {
			out = append(out, registry)
		}
	}
	return out
}

func shouldFallbackToNextHost(code int) bool {
	switch code {
	case http.StatusForbidden, http.StatusNotFound, http.StatusTooManyRequests:
		return true
	default:
		return code >= 500
	}
}

func normalizeMirrorHost(v string) (string, error) {
	s := strings.TrimSpace(v)
	if s == "" {
		return "", nil
	}
	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(u.Host) == "" {
			return "", fmt.Errorf("missing host")
		}
		return strings.ToLower(strings.TrimSpace(u.Host)), nil
	}
	if strings.Contains(s, "/") {
		s = strings.SplitN(s, "/", 2)[0]
	}
	return strings.ToLower(strings.TrimSpace(s)), nil
}

func applyAuthHeader(req *http.Request, cfg authConfig) {
	if req.Header.Get("Authorization") != "" {
		return
	}
	if cfg.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.bearerToken)
		return
	}
	if cfg.username != "" {
		req.SetBasicAuth(cfg.username, cfg.password)
	}
}
