package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/reyoung/afs/pkg/layerformat"
)

func main() {
	var (
		inPath    string
		outPath   string
		mediaType string
	)

	flag.StringVar(&inPath, "in", "", "input layer file path (OCI layer tar+gzip)")
	flag.StringVar(&outPath, "out", "", "output AFSLYR01 file path (default: <in>.afslyr)")
	flag.StringVar(&mediaType, "media-type", layerformat.OCILayerTarGzipMediaType, "input layer media type")
	flag.Parse()

	if strings.TrimSpace(inPath) == "" {
		log.Fatal("-in is required")
	}
	if strings.TrimSpace(outPath) == "" {
		outPath = defaultOutPath(inPath)
	}

	inFile, err := os.Open(inPath)
	if err != nil {
		log.Fatalf("open input layer: %v", err)
	}
	defer inFile.Close()

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer func() {
		if cerr := outFile.Close(); cerr != nil {
			log.Fatalf("close output file: %v", cerr)
		}
	}()

	if err := layerformat.ConvertOCILayer(mediaType, inFile, outFile); err != nil {
		log.Fatalf("convert layer: %v", err)
	}

	st, err := outFile.Stat()
	if err != nil {
		log.Fatalf("stat output file: %v", err)
	}
	fmt.Printf("converted %s -> %s (%d bytes)\n", inPath, outPath, st.Size())
}

func defaultOutPath(inPath string) string {
	if strings.HasSuffix(inPath, ".tar.gz") {
		return strings.TrimSuffix(inPath, ".tar.gz") + ".afslyr"
	}
	if strings.HasSuffix(inPath, ".tgz") {
		return strings.TrimSuffix(inPath, ".tgz") + ".afslyr"
	}
	return inPath + ".afslyr"
}
