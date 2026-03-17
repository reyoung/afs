package afslet

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/afsletpb"
	"github.com/reyoung/afs/pkg/afsmount"
	"github.com/reyoung/afs/pkg/discoverypb"
)

const (
	defaultCPUCores       = int64(1)
	defaultMemoryMB       = int64(256)
	defaultTimeout        = time.Second
	defaultProcessPathEnv = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)

type Config struct {
	MountBinary                 string
	MountInProcess              bool
	RuncBinary                  string
	RuncNoPivot                 bool
	RuncNoNewKeyring            bool
	RuncNoCgroupNS              bool
	RuncNoPIDNS                 bool
	RuncNoIPCNS                 bool
	RuncNoUTSNS                 bool
	UseSudo                     bool
	TarChunk                    int
	DefaultDiscovery            string
	TempDir                     string
	LimitCPUCores               int64
	LimitMemoryMB               int64
	SharedSpillCacheEnabled     bool
	SharedSpillCacheDir         string
	SharedSpillCacheSock        string
	SharedSpillCacheMaxBytes    int64
	SharedSpillCacheBinaryPath  string
	SharedSpillCachePprofListen string
	LayerMountConcurrency       int
	FormatVersion               int
}

type Service struct {
	afsletpb.UnimplementedAfsletServer

	mountBinary                 string
	mountInProcess              bool
	mountRunner                 func(context.Context, afsmount.Config) error
	runcBinary                  string
	runcNoPivot                 bool
	runcNoNewKeyring            bool
	runcNoCgroupNS              bool
	runcNoPIDNS                 bool
	runcNoIPCNS                 bool
	runcNoUTSNS                 bool
	useSudo                     bool
	tarChunk                    int
	defaultDiscovery            string
	tempDir                     string
	mu                          sync.Mutex
	limitCPUCores               int64
	limitMemoryMB               int64
	usedCPUCores                int64
	usedMemoryMB                int64
	runningContainers           int64
	sharedSpillCacheEnabled     bool
	sharedSpillCacheDir         string
	sharedSpillCacheSock        string
	sharedSpillCacheMaxBytes    int64
	sharedSpillCacheBinaryPath  string
	sharedSpillCachePprofListen string
	layerMountConcurrency       int
	formatVersion               int
	resolveImageRuntimeConfig   func(context.Context, string, *afsletpb.StartRequest) (*discoverypb.ImageRuntimeConfig, error)
}

func NewService(cfg Config) *Service {
	s := &Service{
		mountBinary:                 strings.TrimSpace(cfg.MountBinary),
		mountInProcess:              cfg.MountInProcess,
		mountRunner:                 afsmount.Run,
		runcBinary:                  strings.TrimSpace(cfg.RuncBinary),
		runcNoPivot:                 cfg.RuncNoPivot,
		runcNoNewKeyring:            cfg.RuncNoNewKeyring,
		runcNoCgroupNS:              cfg.RuncNoCgroupNS,
		runcNoPIDNS:                 cfg.RuncNoPIDNS,
		runcNoIPCNS:                 cfg.RuncNoIPCNS,
		runcNoUTSNS:                 cfg.RuncNoUTSNS,
		useSudo:                     cfg.UseSudo,
		tarChunk:                    cfg.TarChunk,
		defaultDiscovery:            strings.TrimSpace(cfg.DefaultDiscovery),
		tempDir:                     strings.TrimSpace(cfg.TempDir),
		limitCPUCores:               cfg.LimitCPUCores,
		limitMemoryMB:               cfg.LimitMemoryMB,
		sharedSpillCacheEnabled:     cfg.SharedSpillCacheEnabled,
		sharedSpillCacheDir:         strings.TrimSpace(cfg.SharedSpillCacheDir),
		sharedSpillCacheSock:        strings.TrimSpace(cfg.SharedSpillCacheSock),
		sharedSpillCacheMaxBytes:    cfg.SharedSpillCacheMaxBytes,
		sharedSpillCacheBinaryPath:  strings.TrimSpace(cfg.SharedSpillCacheBinaryPath),
		sharedSpillCachePprofListen: strings.TrimSpace(cfg.SharedSpillCachePprofListen),
		layerMountConcurrency:       cfg.LayerMountConcurrency,
		formatVersion:               cfg.FormatVersion,
	}
	if s.mountBinary == "" {
		s.mountBinary = "afs_mount"
	}
	if s.runcBinary == "" {
		s.runcBinary = "afs_runc"
	}
	if s.tarChunk <= 0 {
		s.tarChunk = 256 * 1024
	}
	if s.limitCPUCores <= 0 {
		s.limitCPUCores = defaultCPUCores
	}
	if s.limitMemoryMB <= 0 {
		s.limitMemoryMB = defaultMemoryMB
	}
	if s.sharedSpillCacheMaxBytes <= 0 {
		s.sharedSpillCacheMaxBytes = 10 << 30
	}
	if s.layerMountConcurrency <= 0 {
		s.layerMountConcurrency = 1
	}
	s.resolveImageRuntimeConfig = s.fetchImageRuntimeConfig
	return s
}

type session struct {
	root       string
	mountpoint string
	workDir    string
	extraDir   string
}

type fileAssembler struct {
	baseDir string

	openFile *os.File
	openMeta *afsletpb.FileEntryBegin
	openPath string
}

func newFileAssembler(baseDir string) *fileAssembler {
	return &fileAssembler{baseDir: baseDir}
}

func (s *Service) Execute(stream afsletpb.Afslet_ExecuteServer) error {
	ctx := stream.Context()
	executeStart := time.Now()
	sess, cleanup, err := newSession(s.tempDir)
	if err != nil {
		return status.Errorf(codes.Internal, "create session: %v", err)
	}
	defer cleanup()

	assembler := newFileAssembler(sess.extraDir)
	defer assembler.Close()

	firstReq, recvErr := stream.Recv()
	if recvErr == io.EOF {
		return status.Error(codes.InvalidArgument, "missing start request")
	}
	if recvErr != nil {
		return recvErr
	}
	firstStart, ok := firstReq.GetPayload().(*afsletpb.ExecuteRequest_Start)
	if !ok || firstStart.Start == nil {
		return status.Error(codes.InvalidArgument, "first request must be start")
	}
	start := firstStart.Start
	if err := validateStartRequest(start); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}

	reserveStart := time.Now()
	release, reserveMs, reserveLockMs, reserveErr := s.reserveResourcesWithTiming(start.GetCpuCores(), start.GetMemoryMb())
	admissionQueueWaitMs := time.Since(reserveStart).Milliseconds() - reserveMs
	if reserveErr != nil {
		return status.Errorf(codes.ResourceExhausted, "%v", reserveErr)
	}
	defer release()

	if err := stream.Send(&afsletpb.ExecuteResponse{Payload: &afsletpb.ExecuteResponse_Accepted{Accepted: &afsletpb.Accepted{Accepted: true}}}); err != nil {
		return err
	}
	_ = s.sendLog(stream, "timing", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=execute_request_enter ms=%d", time.Since(executeStart).Milliseconds()))
	_ = s.sendLog(stream, "timing", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=reserve_resources ms=%d lock_wait_ms=%d admission_queue_wait_ms=%d ok=true", reserveMs, reserveLockMs, admissionQueueWaitMs))

	for {
		req, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return recvErr
		}
		switch p := req.GetPayload().(type) {
		case *afsletpb.ExecuteRequest_Start:
			return status.Error(codes.InvalidArgument, "duplicate start")
		case *afsletpb.ExecuteRequest_FileBegin:
			if err := assembler.Begin(p.FileBegin); err != nil {
				return status.Errorf(codes.InvalidArgument, "file_begin: %v", err)
			}
		case *afsletpb.ExecuteRequest_FileChunk:
			if err := assembler.Chunk(p.FileChunk); err != nil {
				return status.Errorf(codes.InvalidArgument, "file_chunk: %v", err)
			}
		case *afsletpb.ExecuteRequest_FileEnd:
			if err := assembler.End(); err != nil {
				return status.Errorf(codes.InvalidArgument, "file_end: %v", err)
			}
		default:
			return status.Error(codes.InvalidArgument, "unknown request payload")
		}
	}
	if err := assembler.End(); err != nil {
		return status.Errorf(codes.InvalidArgument, "finalize extra-dir files: %v", err)
	}

	logf := func(source string, message string) {
		_ = s.sendLog(stream, source, message)
	}
	runRes := s.runCommand(ctx, sess, start, logf)
	if err := s.sendResult(stream, runRes); err != nil {
		return err
	}

	upperDir := filepath.Join(sess.workDir, "writable-upper")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		return status.Errorf(codes.Internal, "ensure writable-upper: %v", err)
	}
	if err := s.sendTar(stream, upperDir); err != nil {
		return err
	}
	return stream.Send(&afsletpb.ExecuteResponse{Payload: &afsletpb.ExecuteResponse_Done{Done: &afsletpb.Done{}}})
}

func (s *Service) GetRuntimeStatus(ctx context.Context, req *afsletpb.GetRuntimeStatusRequest) (*afsletpb.GetRuntimeStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	availCPU := s.limitCPUCores - s.usedCPUCores
	if availCPU < 0 {
		availCPU = 0
	}
	availMem := s.limitMemoryMB - s.usedMemoryMB
	if availMem < 0 {
		availMem = 0
	}
	return &afsletpb.GetRuntimeStatusResponse{
		RunningContainers: s.runningContainers,
		LimitCpuCores:     s.limitCPUCores,
		LimitMemoryMb:     s.limitMemoryMB,
		UsedCpuCores:      s.usedCPUCores,
		UsedMemoryMb:      s.usedMemoryMB,
		AvailableCpuCores: availCPU,
		AvailableMemoryMb: availMem,
	}, nil
}

type commandResult struct {
	Success  bool
	ExitCode int32
	Err      string
}

type runtimeProcessConfig struct {
	command    []string
	env        []string
	workingDir string
	user       string
}

func (s *Service) addRunningContainers(delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runningContainers += delta
	if s.runningContainers < 0 {
		s.runningContainers = 0
	}
}

func (s *Service) reserveResourcesWithTiming(cpu int64, memoryMB int64) (func(), int64, int64, error) {
	lockStart := time.Now()
	s.mu.Lock()
	lockWaitMs := time.Since(lockStart).Milliseconds()

	resourceCheckStart := time.Now()
	if cpu > s.limitCPUCores {
		s.mu.Unlock()
		return nil, lockWaitMs, 0, fmt.Errorf("requested cpu=%d exceeds limit=%d", cpu, s.limitCPUCores)
	}
	if memoryMB > s.limitMemoryMB {
		s.mu.Unlock()
		return nil, lockWaitMs, 0, fmt.Errorf("requested memory_mb=%d exceeds limit=%d", memoryMB, s.limitMemoryMB)
	}
	remainCPU := s.limitCPUCores - s.usedCPUCores
	remainMem := s.limitMemoryMB - s.usedMemoryMB
	if cpu > remainCPU || memoryMB > remainMem {
		s.mu.Unlock()
		admissionQueueWaitMs := time.Since(resourceCheckStart).Milliseconds()
		return nil, lockWaitMs, admissionQueueWaitMs, fmt.Errorf("insufficient resources: request cpu=%d memory_mb=%d, available cpu=%d memory_mb=%d", cpu, memoryMB, remainCPU, remainMem)
	}

	s.usedCPUCores += cpu
	s.usedMemoryMB += memoryMB
	resourceCheckMs := time.Since(resourceCheckStart).Milliseconds()

	release := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.usedCPUCores -= cpu
		if s.usedCPUCores < 0 {
			s.usedCPUCores = 0
		}
		s.usedMemoryMB -= memoryMB
		if s.usedMemoryMB < 0 {
			s.usedMemoryMB = 0
		}
	}

	s.mu.Unlock()
	return release, lockWaitMs, resourceCheckMs, nil
}

func (s *Service) reserveResources(cpu int64, memoryMB int64) (func(), error) {
	release, _, _, err := s.reserveResourcesWithTiming(cpu, memoryMB)
	return release, err
}

func validateStartRequest(start *afsletpb.StartRequest) error {
	if start == nil {
		return fmt.Errorf("missing start request")
	}
	if _, _, err := normalizeImageAndTag(start.GetImage(), start.GetTag()); err != nil {
		return err
	}
	if strings.TrimSpace(start.GetImage()) == "" {
		return fmt.Errorf("start.image is required")
	}
	if start.GetCpuCores() <= 0 {
		return fmt.Errorf("start.cpu_cores must be > 0")
	}
	if start.GetMemoryMb() <= 0 {
		return fmt.Errorf("start.memory_mb must be > 0")
	}
	return nil
}

func (s *Service) runCommand(ctx context.Context, sess *session, start *afsletpb.StartRequest, logf func(string, string)) commandResult {
	image, tag, normErr := normalizeImageAndTag(start.GetImage(), start.GetTag())
	if normErr != nil {
		return commandResult{Success: false, ExitCode: -1, Err: normErr.Error()}
	}
	cpu := start.GetCpuCores()
	memory := start.GetMemoryMb()
	if cpu <= 0 {
		return commandResult{Success: false, ExitCode: -1, Err: "start.cpu_cores must be > 0"}
	}
	if memory <= 0 {
		return commandResult{Success: false, ExitCode: -1, Err: "start.memory_mb must be > 0"}
	}
	if logf != nil {
		logf("session", fmt.Sprintf("extra-dir prepared at %s", sess.extraDir))
	}

	discoveryAddr := pickDiscoveryAddr(start.GetDiscoveryAddr(), s.defaultDiscovery)
	imageRuntimeConfig, err := s.resolveImageRuntimeConfig(ctx, discoveryAddr, start)
	if err != nil {
		return commandResult{Success: false, ExitCode: -1, Err: fmt.Sprintf("resolve image runtime config: %v", err)}
	}
	processCfg, err := buildRuntimeProcessConfig(start, imageRuntimeConfig)
	if err != nil {
		return commandResult{Success: false, ExitCode: -1, Err: err.Error()}
	}
	if logf != nil {
		logf("image", fmt.Sprintf("resolved entrypoint=%q cmd=%q env=%d working_dir=%q user=%q", imageRuntimeConfig.GetEntrypoint(), imageRuntimeConfig.GetCmd(), len(imageRuntimeConfig.GetEnv()), imageRuntimeConfig.GetWorkingDir(), imageRuntimeConfig.GetUser()))
		logf("request", fmt.Sprintf("image=%s tag=%s requested_cmd=%q resolved_cmd=%q env=%d cwd=%q user=%q cpu=%d memory_mb=%d timeout_ms=%d runc_no_pivot=%t runc_no_new_keyring=%t runc_no_cgroup_ns=%t runc_no_pid_ns=%t runc_no_ipc_ns=%t runc_no_uts_ns=%t", start.GetImage(), start.GetTag(), start.GetCommand(), processCfg.command, len(processCfg.env), processCfg.workingDir, processCfg.user, start.GetCpuCores(), start.GetMemoryMb(), start.GetTimeoutMs(), s.runcNoPivot, s.runcNoNewKeyring, s.runcNoCgroupNS, s.runcNoPIDNS, s.runcNoIPCNS, s.runcNoUTSNS))
		logf("resource", fmt.Sprintf("reserved cpu=%d memory_mb=%d", cpu, memory))
	}

	mountCfg := afsmount.Config{
		Mountpoint:                  sess.mountpoint,
		WorkDir:                     sess.workDir,
		ExtraDir:                    sess.extraDir,
		MountProcDev:                false,
		Image:                       image,
		Tag:                         tag,
		DiscoveryAddr:               discoveryAddr,
		NodeID:                      strings.TrimSpace(start.GetNodeId()),
		PlatformOS:                  strings.TrimSpace(start.GetPlatformOs()),
		PlatformArch:                strings.TrimSpace(start.GetPlatformArch()),
		PlatformVariant:             strings.TrimSpace(start.GetPlatformVariant()),
		ForceLocalFetch:             start.GetForceLocalFetch(),
		SharedSpillCacheEnabled:     s.sharedSpillCacheEnabled,
		SharedSpillCacheDir:         s.sharedSpillCacheDir,
		SharedSpillCacheSock:        s.sharedSpillCacheSock,
		SharedSpillCacheMaxBytes:    s.sharedSpillCacheMaxBytes,
		SharedSpillCacheBinaryPath:  s.sharedSpillCacheBinaryPath,
		SharedSpillCachePprofListen: s.sharedSpillCachePprofListen,
		LayerMountConcurrency:       s.layerMountConcurrency,
		FormatVersion:               s.formatVersion,
	}
	mountArgs := []string{
		"-mountpoint", sess.mountpoint,
		"-work-dir", sess.workDir,
		"-extra-dir", sess.extraDir,
		"-mount-proc-dev=false",
		"-image", image,
	}
	if strings.TrimSpace(tag) != "" {
		mountArgs = append(mountArgs, "-tag", tag)
	}
	if discoveryAddr != "" {
		mountArgs = append(mountArgs, "-discovery-addr", discoveryAddr)
	}
	if start.GetForceLocalFetch() {
		mountArgs = append(mountArgs, "-force-local-fetch")
	}
	if mountCfg.NodeID != "" {
		mountArgs = append(mountArgs, "-node-id", mountCfg.NodeID)
	}
	if mountCfg.PlatformOS != "" {
		mountArgs = append(mountArgs, "-platform-os", mountCfg.PlatformOS)
	}
	if mountCfg.PlatformArch != "" {
		mountArgs = append(mountArgs, "-platform-arch", mountCfg.PlatformArch)
	}
	if mountCfg.PlatformVariant != "" {
		mountArgs = append(mountArgs, "-platform-variant", mountCfg.PlatformVariant)
	}
	if s.sharedSpillCacheEnabled {
		mountArgs = append(mountArgs, "-shared-spill-cache")
		if s.sharedSpillCacheDir != "" {
			mountArgs = append(mountArgs, "-shared-spill-cache-dir", s.sharedSpillCacheDir)
		}
		if s.sharedSpillCacheSock != "" {
			mountArgs = append(mountArgs, "-shared-spill-cache-sock", s.sharedSpillCacheSock)
		}
		if s.sharedSpillCacheMaxBytes > 0 {
			mountArgs = append(mountArgs, "-shared-spill-cache-max-bytes", strconv.FormatInt(s.sharedSpillCacheMaxBytes, 10))
		}
		if s.sharedSpillCacheBinaryPath != "" {
			mountArgs = append(mountArgs, "-shared-spill-cache-binary", s.sharedSpillCacheBinaryPath)
		}
		if s.sharedSpillCachePprofListen != "" {
			mountArgs = append(mountArgs, "-shared-spill-cache-pprof-listen", s.sharedSpillCachePprofListen)
		}
	}
	if s.layerMountConcurrency > 0 {
		mountArgs = append(mountArgs, "-layer-mount-concurrency", strconv.Itoa(s.layerMountConcurrency))
	}

	mountWait := make(chan error, 1)
	var mountReady <-chan struct{}
	var mountStdout *processLogWriter
	var mountStderr *processLogWriter
	stopMount := func() {}
	var stopMountOnce sync.Once
	stopMountSafe := func() { stopMountOnce.Do(stopMount) }
	mountStart := time.Now()

	if s.mountInProcess {
		readyCh := make(chan struct{}, 1)
		mountReady = readyCh
		mountCfg.OnReady = func() {
			select {
			case readyCh <- struct{}{}:
			default:
			}
		}
		mountCtx, cancelMount := context.WithCancel(ctx)
		mountDone := make(chan struct{})
		var mountErr error
		go func() {
			mountErr = s.mountRunner(mountCtx, mountCfg)
			select {
			case mountWait <- mountErr:
			default:
			}
			close(mountDone)
		}()
		stopMount = func() {
			cancelMount()
			select {
			case <-mountDone:
				if mountErr != nil && !errors.Is(mountErr, context.Canceled) && logf != nil {
					logf("mount", fmt.Sprintf("in-process mount exited: %v", mountErr))
				}
			case <-time.After(10 * time.Second):
				if logf != nil {
					logf("mount", "in-process mount stop timeout")
				}
			}
		}
		if logf != nil {
			logf("mount", fmt.Sprintf("started in-process mount for image=%s tag=%s", image, tag))
		}
	} else {
		mountCmd := s.newCommandContext(ctx, s.mountBinary, mountArgs...)
		mountStdout = newProcessLogWriter("mount:stdout", logf)
		mountStderr = newProcessLogWriter("mount:stderr", logf)
		defer mountStdout.Flush()
		defer mountStderr.Flush()
		mountCmd.Stdout = mountStdout
		mountCmd.Stderr = mountStderr
		if err := mountCmd.Start(); err != nil {
			return commandResult{Success: false, ExitCode: -1, Err: fmt.Sprintf("start afs_mount: %v", err)}
		}
		go func() { mountWait <- mountCmd.Wait() }()
		stopMount = func() { _ = terminateProcess(mountCmd, mountWait) }
		if logf != nil {
			logf("mount", fmt.Sprintf("started: %s %s", s.mountBinary, strings.Join(mountArgs, " ")))
		}
	}

	waitMountReadyStart := time.Now()
	progressLogf := func(msg string) {
		if logf != nil {
			logf("mount", msg)
		}
	}
	var readyErr error
	if mountReady != nil {
		readyErr = waitForInProcessMountReady(mountWait, mountReady, 120*time.Second, progressLogf)
	} else {
		readyErr = waitForMountReady(sess.mountpoint, mountWait, 120*time.Second, progressLogf)
	}
	if readyErr != nil {
		if logf != nil {
			logf("mount", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=wait_mount_ready ms=%d ok=false", time.Since(waitMountReadyStart).Milliseconds()))
		}
		stopMountSafe()
		if mountStdout != nil && mountStderr != nil {
			combined := strings.TrimSpace(mountStdout.String() + "\n" + mountStderr.String())
			if combined != "" {
				readyErr = fmt.Errorf("%w: %s", readyErr, combined)
			}
		}
		return commandResult{Success: false, ExitCode: -1, Err: fmt.Sprintf("mount not ready: %v", readyErr)}
	}
	if logf != nil {
		logf("mount", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=wait_mount_ready ms=%d ok=true", time.Since(waitMountReadyStart).Milliseconds()))
		logf("mount", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=mount_start_to_ready ms=%d", time.Since(mountStart).Milliseconds()))
		logf("mount", "mountpoint is ready")
	}

	timeout := defaultTimeout
	if start.GetTimeoutMs() > 0 {
		timeout = time.Duration(start.GetTimeoutMs()) * time.Millisecond
	}

	runcArgs := []string{
		"-rootfs", sess.mountpoint,
		"-cpu", strconv.FormatInt(cpu, 10),
		"-memory-mb", strconv.FormatInt(memory, 10),
		"-timeout", timeout.String(),
	}
	if processCfg.user != "" {
		runcArgs = append(runcArgs, "-user", processCfg.user)
	}
	if processCfg.workingDir != "" {
		runcArgs = append(runcArgs, "-cwd", processCfg.workingDir)
	}
	for _, env := range processCfg.env {
		runcArgs = append(runcArgs, "-env", env)
	}
	if s.runcNoPivot {
		runcArgs = append(runcArgs, "-no-pivot")
	}
	if s.runcNoNewKeyring {
		runcArgs = append(runcArgs, "-no-new-keyring")
	}
	if s.runcNoCgroupNS {
		runcArgs = append(runcArgs, "-no-cgroup-ns")
	}
	if s.runcNoPIDNS {
		runcArgs = append(runcArgs, "-no-pid-ns")
	}
	if s.runcNoIPCNS {
		runcArgs = append(runcArgs, "-no-ipc-ns")
	}
	if s.runcNoUTSNS {
		runcArgs = append(runcArgs, "-no-uts-ns")
	}
	runcArgs = append(runcArgs, "--")
	runcArgs = append(runcArgs, processCfg.command...)
	runcCmd := s.newCommandContext(ctx, s.runcBinary, runcArgs...)
	runcStdout := newProcessLogWriter("runc:stdout", logf)
	runcStderr := newProcessLogWriter("runc:stderr", logf)
	defer runcStdout.Flush()
	defer runcStderr.Flush()
	runcCmd.Stdout = runcStdout
	runcCmd.Stderr = runcStderr
	if logf != nil {
		logf("runc", fmt.Sprintf("running: %s %s", s.runcBinary, strings.Join(runcArgs, " ")))
	}
	runcStart := time.Now()
	if err := runcCmd.Start(); err != nil {
		stopMountSafe()
		_ = s.tryForceUmount(sess.mountpoint)
		if logf != nil {
			logf("runc", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=runc_start ms=%d ok=false", time.Since(runcStart).Milliseconds()))
		}
		return commandResult{Success: false, ExitCode: -1, Err: fmt.Sprintf("start afs_runc: %v", err)}
	}
	if logf != nil {
		logf("runc", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=runc_start ms=%d ok=true", time.Since(runcStart).Milliseconds()))
	}
	s.addRunningContainers(1)
	defer s.addRunningContainers(-1)
	runcWaitStart := time.Now()
	runErr := runcCmd.Wait()
	if logf != nil {
		logf("runc", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=runc_wait ms=%d", time.Since(runcWaitStart).Milliseconds()))
	}

	stopMountStarted := time.Now()
	stopMountSafe()
	if logf != nil {
		logf("mount", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=stop_mount_runner ms=%d", time.Since(stopMountStarted).Milliseconds()))
		logf("mount", fmt.Sprintf("__AFS_AFSLET_TIMING__ phase=mount_lifetime ms=%d", time.Since(mountStart).Milliseconds()))
	}
	if umErr := s.tryForceUmount(sess.mountpoint); umErr != nil {
		if logf != nil {
			logf("cleanup", fmt.Sprintf("umount warning: %v", umErr))
		}
	} else {
		if logf != nil {
			logf("cleanup", "mountpoint unmounted")
		}
	}

	if runErr != nil {
		exitCode := int32(1)
		var ex *exec.ExitError
		if errors.As(runErr, &ex) {
			exitCode = int32(ex.ExitCode())
		}
		msg := strings.TrimSpace(runcStdout.String() + "\n" + runcStderr.String())
		if msg == "" {
			msg = runErr.Error()
		}
		if logf != nil {
			logf("runc", fmt.Sprintf("failed exit=%d", exitCode))
		}
		return commandResult{Success: false, ExitCode: exitCode, Err: msg}
	}
	if logf != nil {
		logf("runc", "completed successfully")
	}
	return commandResult{Success: true, ExitCode: 0, Err: strings.TrimSpace(runcStdout.String())}
}

func (s *Service) fetchImageRuntimeConfig(ctx context.Context, discoveryAddr string, start *afsletpb.StartRequest) (*discoverypb.ImageRuntimeConfig, error) {
	discoveryAddr = strings.TrimSpace(discoveryAddr)
	if discoveryAddr == "" {
		return &discoverypb.ImageRuntimeConfig{}, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(callCtx, discoveryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("dial discovery %s: %w", discoveryAddr, err)
	}
	defer conn.Close()

	client := discoverypb.NewServiceDiscoveryClient(conn)
	resp, err := client.ResolveImage(callCtx, &discoverypb.ResolveImageRequest{
		Image:           start.GetImage(),
		Tag:             start.GetTag(),
		PlatformOs:      strings.TrimSpace(start.GetPlatformOs()),
		PlatformArch:    strings.TrimSpace(start.GetPlatformArch()),
		PlatformVariant: strings.TrimSpace(start.GetPlatformVariant()),
	})
	if err != nil {
		return nil, err
	}
	if resp.GetRuntimeConfig() == nil {
		return &discoverypb.ImageRuntimeConfig{}, nil
	}
	return resp.GetRuntimeConfig(), nil
}

func buildRuntimeProcessConfig(start *afsletpb.StartRequest, imageCfg *discoverypb.ImageRuntimeConfig) (runtimeProcessConfig, error) {
	if start == nil {
		return runtimeProcessConfig{}, fmt.Errorf("missing start request")
	}
	if imageCfg == nil {
		imageCfg = &discoverypb.ImageRuntimeConfig{}
	}
	command, err := resolveCommandArgs(start.GetCommand(), imageCfg.GetEntrypoint(), imageCfg.GetCmd())
	if err != nil {
		return runtimeProcessConfig{}, err
	}
	return runtimeProcessConfig{
		command:    command,
		env:        normalizeProcessEnv(imageCfg.GetEnv(), start.GetEnv()),
		workingDir: normalizeWorkingDir(imageCfg.GetWorkingDir()),
		user:       strings.TrimSpace(imageCfg.GetUser()),
	}, nil
}

func resolveCommandArgs(requestedCmd, entrypoint, imageCmd []string) ([]string, error) {
	requestedCmd = append([]string(nil), requestedCmd...)
	entrypoint = append([]string(nil), entrypoint...)
	imageCmd = append([]string(nil), imageCmd...)

	if len(requestedCmd) > 0 {
		if len(entrypoint) > 0 {
			return append(entrypoint, requestedCmd...), nil
		}
		return requestedCmd, nil
	}
	if len(entrypoint) == 0 && len(imageCmd) == 0 {
		return nil, fmt.Errorf("no command resolved from request or image config")
	}
	return append(entrypoint, imageCmd...), nil
}

func normalizeProcessEnv(envSets ...[]string) []string {
	merged := make([]string, 0)
	for _, envSet := range envSets {
		merged = append(merged, envSet...)
	}
	out := dedupeEnvKeepLast(merged)
	if !hasEnvKey(out, "PATH") {
		out = append(out, defaultProcessPathEnv)
	}
	return out
}

func dedupeEnvKeepLast(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	reversed := make([]string, 0, len(in))
	for i := len(in) - 1; i >= 0; i-- {
		v := strings.TrimSpace(in[i])
		if v == "" {
			continue
		}
		key := envKey(v)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		reversed = append(reversed, v)
	}
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	return reversed
}

func hasEnvKey(env []string, key string) bool {
	for _, entry := range env {
		if envKey(entry) == key {
			return true
		}
	}
	return false
}

func envKey(entry string) string {
	if idx := strings.IndexByte(entry, '='); idx >= 0 {
		return entry[:idx]
	}
	return entry
}

func normalizeWorkingDir(workingDir string) string {
	workingDir = strings.TrimSpace(workingDir)
	if workingDir == "" {
		return "/"
	}
	if !strings.HasPrefix(workingDir, "/") {
		return "/" + workingDir
	}
	return workingDir
}

func (s *Service) sendLog(stream afsletpb.Afslet_ExecuteServer, source string, message string) error {
	return stream.Send(&afsletpb.ExecuteResponse{Payload: &afsletpb.ExecuteResponse_Log{Log: &afsletpb.LogMessage{Source: source, Message: message}}})
}

func (s *Service) sendResult(stream afsletpb.Afslet_ExecuteServer, res commandResult) error {
	return stream.Send(&afsletpb.ExecuteResponse{Payload: &afsletpb.ExecuteResponse_Result{Result: &afsletpb.Result{Success: res.Success, ExitCode: res.ExitCode, Error: res.Err}}})
}

func (s *Service) sendTar(stream afsletpb.Afslet_ExecuteServer, dir string) error {
	if err := stream.Send(&afsletpb.ExecuteResponse{Payload: &afsletpb.ExecuteResponse_TarHeader{TarHeader: &afsletpb.TarHeader{Name: "writable-upper.tar.gz"}}}); err != nil {
		return err
	}

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- writeTarGz(dir, pw)
	}()

	buf := make([]byte, s.tarChunk)
	for {
		n, err := pr.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if sendErr := stream.Send(&afsletpb.ExecuteResponse{Payload: &afsletpb.ExecuteResponse_TarChunk{TarChunk: &afsletpb.TarChunk{Data: chunk}}}); sendErr != nil {
				_ = pr.Close()
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read tar stream: %v", err)
		}
	}
	if err := <-errCh; err != nil {
		return status.Errorf(codes.Internal, "build tar.gz: %v", err)
	}
	return nil
}

func writeTarGz(root string, w *io.PipeWriter) error {
	defer w.Close()
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	defer func() {
		_ = tw.Close()
		_ = gz.Close()
	}()

	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		var link string
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if info.IsDir() && !strings.HasSuffix(hdr.Name, "/") {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			_, cpErr := io.Copy(tw, f)
			_ = f.Close()
			if cpErr != nil {
				return cpErr
			}
		}
		return nil
	})
}

func (b *fileAssembler) Begin(meta *afsletpb.FileEntryBegin) error {
	if meta == nil {
		return fmt.Errorf("nil meta")
	}
	if err := b.End(); err != nil {
		return err
	}
	target, err := safeJoin(b.baseDir, meta.GetPath())
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	b.openMeta = meta
	b.openPath = target

	switch meta.GetType() {
	case afsletpb.FileType_FILE_TYPE_DIR:
		mode := fileMode(meta.GetMode(), true)
		if err := os.MkdirAll(target, mode); err != nil {
			return err
		}
		return applyAttrs(target, meta, false)
	case afsletpb.FileType_FILE_TYPE_SYMLINK:
		if meta.GetSymlinkTarget() == "" {
			return fmt.Errorf("symlink_target is required")
		}
		_ = os.Remove(target)
		if err := os.Symlink(meta.GetSymlinkTarget(), target); err != nil {
			return err
		}
		return applyAttrs(target, meta, true)
	case afsletpb.FileType_FILE_TYPE_FILE:
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_APPEND, fileMode(meta.GetMode(), false))
		if err != nil {
			return err
		}
		b.openFile = f
		return nil
	default:
		return fmt.Errorf("unsupported file type: %v", meta.GetType())
	}
}

func (b *fileAssembler) Chunk(chunk *afsletpb.FileChunk) error {
	if chunk == nil {
		return fmt.Errorf("nil chunk")
	}
	if b.openFile == nil {
		if len(chunk.GetData()) == 0 {
			return nil
		}
		return fmt.Errorf("received file_chunk without open file")
	}
	_, err := b.openFile.Write(chunk.GetData())
	return err
}

func (b *fileAssembler) End() error {
	if b.openFile == nil {
		return nil
	}
	if err := b.openFile.Close(); err != nil {
		return err
	}
	b.openFile = nil
	if b.openMeta != nil {
		if err := applyAttrs(b.openPath, b.openMeta, false); err != nil {
			return err
		}
	}
	b.openMeta = nil
	b.openPath = ""
	return nil
}

func (b *fileAssembler) Close() {
	if b.openFile != nil {
		_ = b.openFile.Close()
		b.openFile = nil
	}
}

func applyAttrs(path string, meta *afsletpb.FileEntryBegin, symlink bool) error {
	if meta == nil {
		return nil
	}
	if symlink {
		if err := os.Lchown(path, int(meta.GetUid()), int(meta.GetGid())); err != nil && !errors.Is(err, syscall.EPERM) {
			return err
		}
		return nil
	}
	if err := os.Chmod(path, fileMode(meta.GetMode(), meta.GetType() == afsletpb.FileType_FILE_TYPE_DIR)); err != nil {
		return err
	}
	if err := os.Chown(path, int(meta.GetUid()), int(meta.GetGid())); err != nil && !errors.Is(err, syscall.EPERM) {
		return err
	}
	if meta.GetMtimeUnix() > 0 {
		t := time.Unix(meta.GetMtimeUnix(), 0)
		if err := os.Chtimes(path, t, t); err != nil {
			return err
		}
	}
	return nil
}

func fileMode(v uint32, isDir bool) os.FileMode {
	if v != 0 {
		return os.FileMode(v)
	}
	if isDir {
		return 0o755
	}
	return 0o644
}

func safeJoin(base string, rel string) (string, error) {
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return "", fmt.Errorf("path is empty")
	}
	clean := filepath.Clean("/" + rel)
	clean = strings.TrimPrefix(clean, "/")
	if clean == "." || clean == "" {
		return "", fmt.Errorf("invalid path %q", rel)
	}
	target := filepath.Join(base, clean)
	prefix := base + string(os.PathSeparator)
	if target != base && !strings.HasPrefix(target, prefix) {
		return "", fmt.Errorf("path escapes base dir")
	}
	return target, nil
}

func newSession(tempDir string) (*session, func(), error) {
	rootBase := strings.TrimSpace(tempDir)
	if rootBase != "" {
		if err := os.MkdirAll(rootBase, 0o755); err != nil {
			return nil, nil, err
		}
	}
	root, err := os.MkdirTemp(rootBase, "afslet-session-*")
	if err != nil {
		return nil, nil, err
	}
	s := &session{
		root:       root,
		mountpoint: filepath.Join(root, "mountpoint"),
		workDir:    filepath.Join(root, "work"),
		extraDir:   filepath.Join(root, "extra"),
	}
	for _, p := range []string{s.mountpoint, s.workDir, s.extraDir} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			_ = os.RemoveAll(root)
			return nil, nil, err
		}
	}
	cleanup := func() { _ = os.RemoveAll(root) }
	return s, cleanup, nil
}

func (s *Service) newCommandContext(ctx context.Context, binary string, args ...string) *exec.Cmd {
	if s.useSudo {
		all := append([]string{binary}, args...)
		return exec.CommandContext(ctx, "sudo", all...)
	}
	return exec.CommandContext(ctx, binary, args...)
}

func waitForMountReady(mountpoint string, mountWait <-chan error, timeout time.Duration, onProgress func(string)) error {
	deadline := time.Now().Add(timeout)
	lastProgress := time.Time{}
	for {
		if err, exited := tryReadMountWait(mountWait); exited {
			return err
		}
		mounted, err := isMounted(mountpoint)
		if err == nil && mounted {
			return nil
		}
		if err, exited := tryReadMountWait(mountWait); exited {
			return err
		}
		if onProgress != nil && (lastProgress.IsZero() || time.Since(lastProgress) >= 2*time.Second) {
			lastProgress = time.Now()
			info := "pending"
			if st, stErr := os.Stat(mountpoint); stErr == nil {
				info = fmt.Sprintf("dir-mode=%o", st.Mode().Perm())
			} else {
				info = fmt.Sprintf("stat-err=%v", stErr)
			}
			onProgress(fmt.Sprintf("waiting for mountpoint %s ... (%s)", mountpoint, info))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for mount")
		}
		sleep := 200 * time.Millisecond
		if remaining := time.Until(deadline); remaining > 0 && remaining < sleep {
			sleep = remaining
		}
		select {
		case err := <-mountWait:
			if err != nil {
				return err
			}
			return fmt.Errorf("mount runner exited before mount became ready")
		case <-time.After(sleep):
		}
	}
}

func waitForInProcessMountReady(mountWait <-chan error, mountReady <-chan struct{}, timeout time.Duration, onProgress func(string)) error {
	if onProgress != nil {
		onProgress("waiting for in-process mount ready ...")
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()
	for {
		select {
		case <-mountReady:
			if err, exited := tryReadMountWait(mountWait); exited {
				return err
			}
			return nil
		case err := <-mountWait:
			if err != nil {
				return err
			}
			return fmt.Errorf("mount runner exited before mount became ready")
		case <-progressTicker.C:
			if onProgress != nil {
				onProgress("waiting for in-process mount ready ...")
			}
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for mount")
		}
	}
}

func tryReadMountWait(mountWait <-chan error) (error, bool) {
	select {
	case err := <-mountWait:
		if err != nil {
			return err, true
		}
		return fmt.Errorf("mount runner exited before mount became ready"), true
	default:
		return nil, false
	}
}

type processLogWriter struct {
	mu     sync.Mutex
	source string
	logf   func(string, string)
	buf    strings.Builder
	line   strings.Builder
}

func newProcessLogWriter(source string, logf func(string, string)) *processLogWriter {
	return &processLogWriter{source: source, logf: logf}
}

func (w *processLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = w.buf.Write(p)
	for _, b := range p {
		if b == '\n' {
			if w.logf != nil {
				w.logf(w.source, w.line.String())
			}
			w.line.Reset()
			continue
		}
		_ = w.line.WriteByte(b)
	}
	return len(p), nil
}

func (w *processLogWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.buf.String()
	if w.line.Len() > 0 {
		if out != "" && !strings.HasSuffix(out, "\n") {
			out += "\n"
		}
		out += w.line.String()
	}
	return strings.TrimSpace(out)
}

func (w *processLogWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.line.Len() == 0 {
		return
	}
	if w.logf != nil {
		w.logf(w.source, w.line.String())
	}
	if w.buf.Len() > 0 && !strings.HasSuffix(w.buf.String(), "\n") {
		_, _ = w.buf.WriteString("\n")
	}
	w.line.Reset()
}

func isMounted(mountpoint string) (bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 5 {
			continue
		}
		if unescapeMountField(fields[4]) == mountpoint {
			return true, nil
		}
	}
	if err := s.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func unescapeMountField(v string) string {
	replacer := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\")
	return replacer.Replace(v)
}

func terminateProcess(cmd *exec.Cmd, waitCh <-chan error) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	select {
	case err := <-waitCh:
		return err
	default:
	}
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return nil
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case err := <-waitCh:
		return err
	case <-time.After(5 * time.Second):
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return nil
		}
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		select {
		case err := <-waitCh:
			return err
		case <-time.After(time.Second):
			return nil
		}
	}
}

func (s *Service) tryForceUmount(mountpoint string) error {
	cmd := s.newCommandContext(context.Background(), "umount", mountpoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(out))
		if text == "" {
			text = err.Error()
		}
		return fmt.Errorf("umount %s: %s", mountpoint, text)
	}
	return nil
}

func normalizeImageAndTag(image string, tag string) (string, string, error) {
	image = strings.TrimSpace(image)
	tag = strings.TrimSpace(tag)
	if image == "" {
		return "", "", fmt.Errorf("start.image is required")
	}
	base, detected := splitImageAndTag(image)
	switch {
	case tag == "" && detected != "":
		return base, detected, nil
	case tag != "" && detected != "":
		if tag != detected {
			return "", "", fmt.Errorf("image tag mismatch: image=%q tag=%q", detected, tag)
		}
		return base, tag, nil
	default:
		return image, tag, nil
	}
}

func splitImageAndTag(image string) (string, string) {
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon > lastSlash {
		return image[:lastColon], image[lastColon+1:]
	}
	return image, ""
}

func pickDiscoveryAddr(req string, def string) string {
	req = strings.TrimSpace(req)
	if req != "" {
		return req
	}
	return strings.TrimSpace(def)
}
