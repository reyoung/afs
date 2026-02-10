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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/reyoung/afs/pkg/afsletpb"
)

const (
	defaultCPUCores = int64(1)
	defaultMemoryMB = int64(256)
	defaultTimeout  = time.Second
)

type Config struct {
	MountBinary      string
	RuncBinary       string
	UseSudo          bool
	TarChunk         int
	DefaultDiscovery string
	TempDir          string
}

type Service struct {
	afsletpb.UnimplementedAfsletServer

	mountBinary      string
	runcBinary       string
	useSudo          bool
	tarChunk         int
	defaultDiscovery string
	tempDir          string
}

func NewService(cfg Config) *Service {
	s := &Service{
		mountBinary:      strings.TrimSpace(cfg.MountBinary),
		runcBinary:       strings.TrimSpace(cfg.RuncBinary),
		useSudo:          cfg.UseSudo,
		tarChunk:         cfg.TarChunk,
		defaultDiscovery: strings.TrimSpace(cfg.DefaultDiscovery),
		tempDir:          strings.TrimSpace(cfg.TempDir),
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
	sess, cleanup, err := newSession(s.tempDir)
	if err != nil {
		return status.Errorf(codes.Internal, "create session: %v", err)
	}
	defer cleanup()

	assembler := newFileAssembler(sess.extraDir)
	defer assembler.Close()

	var start *afsletpb.StartRequest
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
			if start != nil {
				return status.Error(codes.InvalidArgument, "duplicate start")
			}
			start = p.Start
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
	if start == nil {
		return status.Error(codes.InvalidArgument, "missing start request")
	}
	if err := assembler.End(); err != nil {
		return status.Errorf(codes.InvalidArgument, "finalize extra-dir files: %v", err)
	}

	if err := s.sendLog(stream, "session", fmt.Sprintf("extra-dir prepared at %s", sess.extraDir)); err != nil {
		return err
	}
	if err := s.sendLog(stream, "request", fmt.Sprintf("image=%s tag=%s cmd=%q cpu=%d memory_mb=%d timeout_ms=%d", start.GetImage(), start.GetTag(), start.GetCommand(), start.GetCpuCores(), start.GetMemoryMb(), start.GetTimeoutMs())); err != nil {
		return err
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

type commandResult struct {
	Success  bool
	ExitCode int32
	Err      string
}

func (s *Service) runCommand(ctx context.Context, sess *session, start *afsletpb.StartRequest, logf func(string, string)) commandResult {
	image, tag, normErr := normalizeImageAndTag(start.GetImage(), start.GetTag())
	if normErr != nil {
		return commandResult{Success: false, ExitCode: -1, Err: normErr.Error()}
	}
	if strings.TrimSpace(image) == "" {
		return commandResult{Success: false, ExitCode: -1, Err: "start.image is required"}
	}
	if len(start.GetCommand()) == 0 {
		return commandResult{Success: false, ExitCode: -1, Err: "start.command is required"}
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
	if discoveryAddr := pickDiscoveryAddr(start.GetDiscoveryAddr(), s.defaultDiscovery); discoveryAddr != "" {
		mountArgs = append(mountArgs, "-discovery-addr", discoveryAddr)
	}
	if start.GetForcePull() {
		mountArgs = append(mountArgs, "-force-pull")
	}
	if strings.TrimSpace(start.GetNodeId()) != "" {
		mountArgs = append(mountArgs, "-node-id", start.GetNodeId())
	}
	if strings.TrimSpace(start.GetPlatformOs()) != "" {
		mountArgs = append(mountArgs, "-platform-os", start.GetPlatformOs())
	}
	if strings.TrimSpace(start.GetPlatformArch()) != "" {
		mountArgs = append(mountArgs, "-platform-arch", start.GetPlatformArch())
	}
	if strings.TrimSpace(start.GetPlatformVariant()) != "" {
		mountArgs = append(mountArgs, "-platform-variant", start.GetPlatformVariant())
	}

	mountCmd := s.newCommandContext(ctx, s.mountBinary, mountArgs...)
	mountStdout := newProcessLogWriter("mount:stdout", logf)
	mountStderr := newProcessLogWriter("mount:stderr", logf)
	mountCmd.Stdout = mountStdout
	mountCmd.Stderr = mountStderr
	if err := mountCmd.Start(); err != nil {
		return commandResult{Success: false, ExitCode: -1, Err: fmt.Sprintf("start afs_mount: %v", err)}
	}
	if logf != nil {
		logf("mount", fmt.Sprintf("started: %s %s", s.mountBinary, strings.Join(mountArgs, " ")))
	}

	mountWait := make(chan error, 1)
	go func() { mountWait <- mountCmd.Wait() }()

	readyErr := waitForMountReady(sess.mountpoint, mountWait, 120*time.Second, func(msg string) {
		if logf != nil {
			logf("mount", msg)
		}
	})
	if readyErr != nil {
		_ = terminateProcess(mountCmd, mountWait)
		combined := strings.TrimSpace(mountStdout.String() + "\n" + mountStderr.String())
		if combined != "" {
			readyErr = fmt.Errorf("%w: %s", readyErr, combined)
		}
		return commandResult{Success: false, ExitCode: -1, Err: fmt.Sprintf("mount not ready: %v", readyErr)}
	}
	if logf != nil {
		logf("mount", "mountpoint is ready")
	}

	cpu := start.GetCpuCores()
	if cpu <= 0 {
		cpu = defaultCPUCores
	}
	memory := start.GetMemoryMb()
	if memory <= 0 {
		memory = defaultMemoryMB
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
		"--",
	}
	runcArgs = append(runcArgs, start.GetCommand()...)
	runcCmd := s.newCommandContext(ctx, s.runcBinary, runcArgs...)
	runcStdout := newProcessLogWriter("runc:stdout", logf)
	runcStderr := newProcessLogWriter("runc:stderr", logf)
	runcCmd.Stdout = runcStdout
	runcCmd.Stderr = runcStderr
	if logf != nil {
		logf("runc", fmt.Sprintf("running: %s %s", s.runcBinary, strings.Join(runcArgs, " ")))
	}
	if err := runcCmd.Start(); err != nil {
		_ = terminateProcess(mountCmd, mountWait)
		_ = tryForceUmount(sess.mountpoint)
		return commandResult{Success: false, ExitCode: -1, Err: fmt.Sprintf("start afs_runc: %v", err)}
	}
	runErr := runcCmd.Wait()

	_ = terminateProcess(mountCmd, mountWait)
	if umErr := tryForceUmount(sess.mountpoint); umErr != nil {
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
		select {
		case err := <-mountWait:
			if err != nil {
				return err
			}
			return fmt.Errorf("afs_mount exited before mount became ready")
		default:
		}
		mounted, err := isMounted(mountpoint)
		if err == nil && mounted {
			return nil
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
		time.Sleep(200 * time.Millisecond)
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
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-waitCh:
		return err
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		return <-waitCh
	}
}

func tryForceUmount(mountpoint string) error {
	cmd := exec.Command("umount", mountpoint)
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
