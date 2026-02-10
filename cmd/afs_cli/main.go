package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/afsletpb"
)

type config struct {
	addr            string
	statusOnly      bool
	dir             string
	image           string
	tag             string
	out             string
	cpu             int64
	memoryMB        int64
	timeout         time.Duration
	discoveryAddr   string
	forcePull       bool
	nodeID          string
	platformOS      string
	platformArch    string
	platformVariant string
	proxyMaxRetries int64
	proxyBackoff    time.Duration
	chunkSize       int
	grpcTimeout     time.Duration
}

func main() {
	cfg, cmdArgs := parseFlags()
	if err := run(cfg, cmdArgs); err != nil {
		log.Fatalf("afs_cli failed: %v", err)
	}
}

func parseFlags() (config, []string) {
	cfg := config{}
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:61051", "afslet gRPC address")
	flag.BoolVar(&cfg.statusOnly, "status", false, "query runtime status only (running containers)")
	flag.StringVar(&cfg.dir, "dir", "", "directory to upload as extra-dir")
	flag.StringVar(&cfg.image, "image", "", "image name")
	flag.StringVar(&cfg.tag, "tag", "", "image tag")
	flag.StringVar(&cfg.out, "out", "writable-upper.tar.gz", "output tar.gz file path")
	flag.Int64Var(&cfg.cpu, "cpu", 1, "cpu core limit")
	flag.Int64Var(&cfg.memoryMB, "memory-mb", 256, "memory limit in MB")
	flag.DurationVar(&cfg.timeout, "timeout", time.Second, "command timeout")
	flag.StringVar(&cfg.discoveryAddr, "discovery-addr", "", "discovery grpc address for afs_mount")
	flag.BoolVar(&cfg.forcePull, "force-pull", false, "force pull image")
	flag.StringVar(&cfg.nodeID, "node-id", "", "node affinity id")
	flag.StringVar(&cfg.platformOS, "platform-os", "linux", "platform os")
	flag.StringVar(&cfg.platformArch, "platform-arch", "amd64", "platform arch")
	flag.StringVar(&cfg.platformVariant, "platform-variant", "", "platform variant")
	flag.Int64Var(&cfg.proxyMaxRetries, "proxy-dispatch-max-retries", 0, "dispatch retry count for afs_proxy (0 means infinite retries)")
	flag.DurationVar(&cfg.proxyBackoff, "proxy-dispatch-backoff", 50*time.Millisecond, "dispatch retry backoff for afs_proxy")
	flag.IntVar(&cfg.chunkSize, "chunk-size", 256*1024, "upload chunk size bytes")
	flag.DurationVar(&cfg.grpcTimeout, "grpc-timeout", 20*time.Minute, "overall grpc timeout")
	flag.Parse()

	cmdArgs := flag.Args()
	if cfg.statusOnly {
		if cfg.grpcTimeout <= 0 {
			log.Fatal("-grpc-timeout must be > 0")
		}
		return cfg, nil
	}
	if strings.TrimSpace(cfg.dir) == "" {
		log.Fatal("-dir is required")
	}
	if strings.TrimSpace(cfg.image) == "" {
		log.Fatal("-image is required")
	}
	if len(cmdArgs) == 0 {
		log.Fatal("command is required, example: -- /bin/sh -c 'echo ok'")
	}
	if cfg.cpu <= 0 {
		log.Fatal("-cpu must be > 0")
	}
	if cfg.memoryMB <= 0 {
		log.Fatal("-memory-mb must be > 0")
	}
	if cfg.timeout <= 0 {
		log.Fatal("-timeout must be > 0")
	}
	if cfg.chunkSize <= 0 {
		log.Fatal("-chunk-size must be > 0")
	}
	if cfg.proxyMaxRetries < 0 {
		log.Fatal("-proxy-dispatch-max-retries must be >= 0")
	}
	if cfg.proxyBackoff < 0 {
		log.Fatal("-proxy-dispatch-backoff must be >= 0")
	}
	if cfg.grpcTimeout <= 0 {
		log.Fatal("-grpc-timeout must be > 0")
	}
	if err := ensureDir(cfg.dir); err != nil {
		log.Fatalf("invalid -dir: %v", err)
	}
	return cfg, cmdArgs
}

func run(cfg config, cmdArgs []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.grpcTimeout)
	defer cancel()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
	conn, err := grpc.DialContext(
		ctx,
		cfg.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.addr, err)
	}
	defer conn.Close()

	client := afsletpb.NewAfsletClient(conn)
	if cfg.statusOnly {
		resp, err := client.GetRuntimeStatus(ctx, &afsletpb.GetRuntimeStatusRequest{})
		if err != nil {
			return fmt.Errorf("get runtime status: %w", err)
		}
		fmt.Printf("running_containers=%d limit_cpu_cores=%d limit_memory_mb=%d used_cpu_cores=%d used_memory_mb=%d available_cpu_cores=%d available_memory_mb=%d\n",
			resp.GetRunningContainers(),
			resp.GetLimitCpuCores(),
			resp.GetLimitMemoryMb(),
			resp.GetUsedCpuCores(),
			resp.GetUsedMemoryMb(),
			resp.GetAvailableCpuCores(),
			resp.GetAvailableMemoryMb(),
		)
		return nil
	}
	stream, err := client.Execute(ctx)
	if err != nil {
		return fmt.Errorf("execute stream: %w", err)
	}

	if err := sendStart(stream, cfg, cmdArgs); err != nil {
		return err
	}
	if err := uploadDir(stream, cfg.dir, cfg.chunkSize); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close send: %w", err)
	}

	return receiveResponses(stream, cfg.out)
}

func sendStart(stream afsletpb.Afslet_ExecuteClient, cfg config, cmdArgs []string) error {
	req := &afsletpb.ExecuteRequest{Payload: &afsletpb.ExecuteRequest_Start{Start: &afsletpb.StartRequest{
		Image:                   cfg.image,
		Tag:                     cfg.tag,
		Command:                 cmdArgs,
		CpuCores:                cfg.cpu,
		MemoryMb:                cfg.memoryMB,
		TimeoutMs:               cfg.timeout.Milliseconds(),
		DiscoveryAddr:           cfg.discoveryAddr,
		ForcePull:               cfg.forcePull,
		NodeId:                  cfg.nodeID,
		PlatformOs:              cfg.platformOS,
		PlatformArch:            cfg.platformArch,
		PlatformVariant:         cfg.platformVariant,
		ProxyDispatchMaxRetries: cfg.proxyMaxRetries,
		ProxyDispatchBackoffMs:  cfg.proxyBackoff.Milliseconds(),
	}}}
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("send start: %w", err)
	}
	return nil
}

func uploadDir(stream afsletpb.Afslet_ExecuteClient, root string, chunkSize int) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		meta := buildMeta(rel, info)
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			meta.SymlinkTarget = target
		}
		if err := stream.Send(&afsletpb.ExecuteRequest{Payload: &afsletpb.ExecuteRequest_FileBegin{FileBegin: meta}}); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			if err := uploadFileChunks(stream, path, chunkSize); err != nil {
				return err
			}
		}
		if err := stream.Send(&afsletpb.ExecuteRequest{Payload: &afsletpb.ExecuteRequest_FileEnd{FileEnd: &afsletpb.FileEntryEnd{}}}); err != nil {
			return err
		}
		return nil
	})
}

func uploadFileChunks(stream afsletpb.Afslet_ExecuteClient, path string, chunkSize int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, chunkSize)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if sendErr := stream.Send(&afsletpb.ExecuteRequest{Payload: &afsletpb.ExecuteRequest_FileChunk{FileChunk: &afsletpb.FileChunk{Data: chunk}}}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func buildMeta(rel string, info fs.FileInfo) *afsletpb.FileEntryBegin {
	uid, gid := statOwner(info)
	m := &afsletpb.FileEntryBegin{
		Path:      rel,
		Mode:      uint32(info.Mode().Perm()),
		Uid:       uid,
		Gid:       gid,
		MtimeUnix: info.ModTime().Unix(),
	}
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		m.Type = afsletpb.FileType_FILE_TYPE_SYMLINK
	case info.IsDir():
		m.Type = afsletpb.FileType_FILE_TYPE_DIR
	default:
		m.Type = afsletpb.FileType_FILE_TYPE_FILE
	}
	return m
}

func statOwner(info fs.FileInfo) (uint32, uint32) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}
	return st.Uid, st.Gid
}

func receiveResponses(stream afsletpb.Afslet_ExecuteClient, outPath string) error {
	var outFile *os.File
	defer func() {
		if outFile != nil {
			_ = outFile.Close()
		}
	}()

	resultReceived := false
	resultSuccess := false
	resultExitCode := int32(0)
	resultErr := ""

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receive response: %w", err)
		}
		switch p := resp.GetPayload().(type) {
		case *afsletpb.ExecuteResponse_Accepted:
			log.Printf("[accepted] %v", p.Accepted.GetAccepted())
		case *afsletpb.ExecuteResponse_Log:
			log.Printf("[%s] %s", p.Log.GetSource(), p.Log.GetMessage())
		case *afsletpb.ExecuteResponse_Result:
			resultReceived = true
			resultSuccess = p.Result.GetSuccess()
			resultExitCode = p.Result.GetExitCode()
			resultErr = p.Result.GetError()
		case *afsletpb.ExecuteResponse_TarHeader:
			if outFile != nil {
				_ = outFile.Close()
			}
			f, openErr := os.Create(outPath)
			if openErr != nil {
				return fmt.Errorf("create output %s: %w", outPath, openErr)
			}
			outFile = f
			log.Printf("receiving %s -> %s", p.TarHeader.GetName(), outPath)
		case *afsletpb.ExecuteResponse_TarChunk:
			if outFile == nil {
				return fmt.Errorf("received tar chunk before tar header")
			}
			if _, writeErr := outFile.Write(p.TarChunk.GetData()); writeErr != nil {
				return fmt.Errorf("write output tar.gz: %w", writeErr)
			}
		case *afsletpb.ExecuteResponse_Done:
			if outFile != nil {
				if closeErr := outFile.Close(); closeErr != nil {
					return fmt.Errorf("close output tar.gz: %w", closeErr)
				}
				outFile = nil
			}
			if !resultReceived {
				return fmt.Errorf("server finished without result")
			}
			if !resultSuccess {
				return fmt.Errorf("command failed: exit=%d err=%s", resultExitCode, resultErr)
			}
			return nil
		default:
			return fmt.Errorf("unknown response payload")
		}
	}

	if !resultReceived {
		return fmt.Errorf("stream closed without result")
	}
	if !resultSuccess {
		return fmt.Errorf("command failed: exit=%d err=%s", resultExitCode, resultErr)
	}
	return nil
}

func ensureDir(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}
