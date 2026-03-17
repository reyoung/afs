package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/reyoung/afs/pkg/afsletpb"
)

func main() {
	var addr string
	var image string
	var tag string
	var delay time.Duration
	var timeout time.Duration

	flag.StringVar(&addr, "addr", "127.0.0.1:62051", "proxy gRPC address")
	flag.StringVar(&image, "image", "mirror.ccs.tencentyun.com/library/alpine", "image")
	flag.StringVar(&tag, "tag", "3.20", "tag")
	flag.DurationVar(&delay, "delay", 750*time.Millisecond, "delay before abort after accepted/log")
	flag.DurationVar(&timeout, "timeout", 45*time.Second, "dial and rpc timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fail("dial %s: %v", addr, err)
	}
	defer conn.Close()

	client := afsletpb.NewAfsletClient(conn)
	stream, err := client.Execute(ctx)
	if err != nil {
		fail("open execute stream: %v", err)
	}

	startReq := &afsletpb.ExecuteRequest{
		Payload: &afsletpb.ExecuteRequest_Start{
			Start: &afsletpb.StartRequest{
				Image:     image,
				Tag:       tag,
				Command:   []string{"/bin/sh", "-lc", "echo started; sleep 20"},
				CpuCores:  1,
				MemoryMb:  256,
				TimeoutMs: 30000,
			},
		},
	}
	if err := stream.Send(startReq); err != nil {
		fail("send start: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		fail("close send: %v", err)
	}

	accepted := false
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			fail("stream completed before abort")
		}
		if err != nil {
			fail("recv before abort: %v", err)
		}
		switch p := resp.GetPayload().(type) {
		case *afsletpb.ExecuteResponse_Accepted:
			fmt.Printf("accepted=%v\n", p.Accepted.GetAccepted())
			accepted = true
		case *afsletpb.ExecuteResponse_Log:
			fmt.Printf("log[%s]=%s\n", p.Log.GetSource(), oneLine(p.Log.GetMessage()))
			if accepted {
				time.Sleep(delay)
				os.Exit(99)
			}
		}
	}
}

func oneLine(v string) string {
	return strings.ReplaceAll(strings.TrimSpace(v), "\n", " ")
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
