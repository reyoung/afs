package main

import "testing"

func TestParseRegistryTokenPair(t *testing.T) {
	t.Parallel()

	host, token, err := parseRegistryTokenPair("registry-1.docker.io=abc123")
	if err != nil {
		t.Fatalf("parseRegistryTokenPair returned error: %v", err)
	}
	if host != "registry-1.docker.io" {
		t.Fatalf("host=%q, want %q", host, "registry-1.docker.io")
	}
	if token != "abc123" {
		t.Fatalf("token=%q, want %q", token, "abc123")
	}
}

func TestParseRegistryTokenPairInvalid(t *testing.T) {
	t.Parallel()

	if _, _, err := parseRegistryTokenPair("registry-1.docker.io"); err == nil {
		t.Fatalf("expected error for missing token")
	}
	if _, _, err := parseRegistryTokenPair("=abc123"); err == nil {
		t.Fatalf("expected error for missing host")
	}
}

func TestParseRegistryBasicPair(t *testing.T) {
	t.Parallel()

	host, username, password, err := parseRegistryBasicPair("registry-1.docker.io=user:pass")
	if err != nil {
		t.Fatalf("parseRegistryBasicPair returned error: %v", err)
	}
	if host != "registry-1.docker.io" {
		t.Fatalf("host=%q, want %q", host, "registry-1.docker.io")
	}
	if username != "user" {
		t.Fatalf("username=%q, want %q", username, "user")
	}
	if password != "pass" {
		t.Fatalf("password=%q, want %q", password, "pass")
	}

	host, username, password, err = parseRegistryBasicPair("ghcr.io=alice")
	if err != nil {
		t.Fatalf("parseRegistryBasicPair (no password) returned error: %v", err)
	}
	if host != "ghcr.io" || username != "alice" || password != "" {
		t.Fatalf("got (%q, %q, %q), want (%q, %q, %q)", host, username, password, "ghcr.io", "alice", "")
	}
}

func TestParseRegistryBasicPairInvalid(t *testing.T) {
	t.Parallel()

	if _, _, _, err := parseRegistryBasicPair("ghcr.io"); err == nil {
		t.Fatalf("expected error for missing credentials")
	}
	if _, _, _, err := parseRegistryBasicPair("=alice"); err == nil {
		t.Fatalf("expected error for missing host")
	}
	if _, _, _, err := parseRegistryBasicPair("ghcr.io=:pass"); err == nil {
		t.Fatalf("expected error for missing username")
	}
}
