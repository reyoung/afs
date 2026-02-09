package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/reyoung/afs/pkg/registry"
)

func main() {
	var (
		image    string
		tag      string
		username string
		password string
		token    string
		platform string
		outDir   string
		timeout  time.Duration
	)

	flag.StringVar(&image, "image", "", "image name, e.g. nginx or ghcr.io/org/app")
	flag.StringVar(&tag, "tag", "", "image tag (optional; default latest, or use image@sha256:...)")
	flag.StringVar(&username, "username", "", "registry username")
	flag.StringVar(&password, "password", "", "registry password")
	flag.StringVar(&token, "token", "", "registry bearer token")
	flag.StringVar(&platform, "platform", "linux/amd64", "target platform as os/arch[/variant], e.g. linux/amd64")
	flag.StringVar(&outDir, "out-dir", ".", "output directory for downloaded layers")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall timeout")
	flag.Parse()

	if strings.TrimSpace(image) == "" {
		log.Fatal("-image is required")
	}
	if token != "" && username != "" {
		log.Fatal("-token and -username/-password are mutually exclusive")
	}
	if username == "" && password != "" {
		log.Fatal("-password requires -username")
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	ref, err := registry.ParseImageReference(image, tag)
	if err != nil {
		log.Fatalf("parse image reference: %v", err)
	}
	platformOS, platformArch, platformVariant, err := parsePlatform(platform)
	if err != nil {
		log.Fatalf("invalid -platform: %v", err)
	}

	client := registry.NewClient(nil)
	if token != "" {
		if err := client.LoginWithToken(ref.Registry, token); err != nil {
			log.Fatalf("configure token login: %v", err)
		}
	} else if username != "" {
		if err := client.Login(ref.Registry, username, password); err != nil {
			log.Fatalf("configure basic login: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	layers, err := client.GetLayersForPlatform(ctx, image, tag, platformOS, platformArch, platformVariant)
	if err != nil {
		log.Fatalf("get layers: %v", err)
	}
	if len(layers) == 0 {
		log.Printf("no layers found: image=%s reference=%s", ref.Repository, ref.Reference)
		return
	}

	fmt.Printf("pulling %d layers from %s/%s:%s into %s\n", len(layers), ref.Registry, ref.Repository, ref.Reference, outDir)
	for i, layer := range layers {
		printLayerMeta(i, len(layers), layer)
		if err := downloadLayer(ctx, client, image, tag, outDir, i, len(layers), layer.Digest, layer.Size); err != nil {
			log.Fatalf("pull layer %s: %v", layer.Digest, err)
		}
	}

	fmt.Println("done")
}

func parsePlatform(v string) (os string, arch string, variant string, err error) {
	parts := strings.Split(v, "/")
	if len(parts) < 2 || len(parts) > 3 {
		return "", "", "", fmt.Errorf("must be os/arch or os/arch/variant")
	}
	os = strings.TrimSpace(parts[0])
	arch = strings.TrimSpace(parts[1])
	if os == "" || arch == "" {
		return "", "", "", fmt.Errorf("os and arch must not be empty")
	}
	if len(parts) == 3 {
		variant = strings.TrimSpace(parts[2])
	}
	return os, arch, variant, nil
}

func downloadLayer(ctx context.Context, client *registry.Client, image, tag, outDir string, index, total int, digest string, expectedSize int64) error {
	filename := layerFilename(index, digest)
	fullPath := filepath.Join(outDir, filename)

	fmt.Printf("[%d/%d] downloading %s -> %s\n", index+1, total, digest, filename)
	rc, err := client.DownloadLayer(ctx, image, tag, digest)
	if err != nil {
		return err
	}
	defer rc.Close()

	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("create file %s: %w", fullPath, err)
	}
	written, copyErr := io.Copy(f, rc)
	closeErr := f.Close()
	if copyErr != nil {
		return fmt.Errorf("write file %s: %w", fullPath, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close file %s: %w", fullPath, closeErr)
	}

	if expectedSize > 0 && written != expectedSize {
		fmt.Printf("warning: size mismatch for %s: got=%d expected=%d\n", filename, written, expectedSize)
	}
	fmt.Printf("[%d/%d] done %s (%s bytes)\n", index+1, total, filename, strconv.FormatInt(written, 10))
	return nil
}

func layerFilename(index int, digest string) string {
	normalized := strings.ReplaceAll(digest, ":", "_")
	normalized = strings.ReplaceAll(normalized, "/", "_")
	return fmt.Sprintf("%02d_%s.layer", index+1, normalized)
}

func printLayerMeta(index int, total int, layer registry.Layer) {
	compression, unpackHint := layerUnpackHint(layer.MediaType)
	fmt.Printf("[%d/%d] layer meta:\n", index+1, total)
	fmt.Printf("  digest: %s\n", layer.Digest)
	fmt.Printf("  mediaType: %s\n", layer.MediaType)
	fmt.Printf("  size: %d bytes\n", layer.Size)
	fmt.Printf("  compression: %s\n", compression)
	fmt.Printf("  unpack: %s\n", unpackHint)
}

func layerUnpackHint(mediaType string) (compression string, hint string) {
	switch {
	case strings.Contains(mediaType, "+gzip"):
		return "gzip", "gunzip -c <layer-file> | tar -xpf -"
	case strings.Contains(mediaType, "+zstd"):
		return "zstd", "zstd -dc <layer-file> | tar -xpf -"
	case strings.Contains(mediaType, ".tar"):
		return "none", "tar -xpf <layer-file>"
	default:
		return "unknown", "unknown layer format; inspect mediaType and use matching decompressor"
	}
}
