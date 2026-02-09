package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/reyoung/afs/pkg/layerformat"
	"github.com/reyoung/afs/pkg/layerfuse"
)

func main() {
	var (
		inPath     string
		mountpoint string
		debug      bool
	)

	flag.StringVar(&inPath, "in", "", "input AFSLYR01 file path")
	flag.StringVar(&mountpoint, "mountpoint", "", "mount target directory")
	flag.BoolVar(&debug, "debug", false, "enable go-fuse debug logs")
	flag.Parse()

	if inPath == "" {
		log.Fatal("-in is required")
	}
	if mountpoint == "" {
		log.Fatal("-mountpoint is required")
	}

	inFile, err := os.Open(inPath)
	if err != nil {
		log.Fatalf("open input file: %v", err)
	}
	defer inFile.Close()

	if st, err := os.Stat(mountpoint); err != nil {
		log.Fatalf("stat mountpoint: %v", err)
	} else if !st.IsDir() {
		log.Fatalf("mountpoint is not a directory: %s", mountpoint)
	}

	reader, err := layerformat.NewReader(inFile)
	if err != nil {
		log.Fatalf("open AFSLYR01 reader: %v", err)
	}

	root := layerfuse.NewRoot(reader)
	server, err := fusefs.Mount(mountpoint, root, &fusefs.Options{
		MountOptions: fuse.MountOptions{
			Debug:   debug,
			FsName:  fmt.Sprintf("afslyr:%s", inPath),
			Name:    "afslyr",
			Options: []string{"ro"},
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "no FUSE mount utility found") {
			log.Fatalf("mount fuse: %v\nhint: install macFUSE and ensure /Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse exists.\nexample: brew install --cask macfuse", err)
		}
		log.Fatalf("mount fuse: %v", err)
	}

	log.Printf("mounted %s at %s (read-only)", inPath, mountpoint)
	log.Printf("press Ctrl+C to unmount")

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	log.Printf("unmounting %s", mountpoint)
	if err := server.Unmount(); err != nil {
		log.Fatalf("unmount failed: %v", err)
	}
	server.Wait()
	log.Printf("unmounted")
}
