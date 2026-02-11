package afsproxy

import (
	"testing"
	"time"

	"github.com/reyoung/afs/pkg/afsletpb"
)

func TestCandidateLoad(t *testing.T) {
	t.Parallel()
	c := backendCandidate{status: &afsletpb.GetRuntimeStatusResponse{
		LimitCpuCores: 8,
		LimitMemoryMb: 1024,
		UsedCpuCores:  2,
		UsedMemoryMb:  768,
	}}
	got := candidateLoad(c)
	if got < 0.74 || got > 0.76 {
		t.Fatalf("candidateLoad=%f, want about 0.75", got)
	}
}

func TestPickByPowerOfTwoChoicesReturnsCandidate(t *testing.T) {
	t.Parallel()
	s := NewService(Config{AfsletTarget: "127.0.0.1:61051"})
	cands := []backendCandidate{
		{address: "a", status: &afsletpb.GetRuntimeStatusResponse{LimitCpuCores: 4, UsedCpuCores: 3, LimitMemoryMb: 400, UsedMemoryMb: 350}},
		{address: "b", status: &afsletpb.GetRuntimeStatusResponse{LimitCpuCores: 4, UsedCpuCores: 1, LimitMemoryMb: 400, UsedMemoryMb: 100}},
	}
	picked := s.pickByPowerOfTwoChoices(cands)
	if picked.address != "a" && picked.address != "b" {
		t.Fatalf("unexpected candidate: %s", picked.address)
	}
}

func TestComputeBackoff(t *testing.T) {
	t.Parallel()
	s := NewService(Config{})
	b1 := s.computeBackoff(50*time.Millisecond, 1)
	b2 := s.computeBackoff(50*time.Millisecond, 3)
	if b1 < 50*time.Millisecond {
		t.Fatalf("b1 too small: %s", b1)
	}
	if b2 < 150*time.Millisecond {
		t.Fatalf("b2 too small: %s", b2)
	}
}

func TestParseBoolDefaultTrue(t *testing.T) {
	t.Parallel()
	if !parseBoolDefaultTrue("1") {
		t.Fatalf("expected true for 1")
	}
	if !parseBoolDefaultTrue("true") {
		t.Fatalf("expected true for true")
	}
	if !parseBoolDefaultTrue("") {
		t.Fatalf("expected true for empty")
	}
	if parseBoolDefaultTrue("no") {
		t.Fatalf("expected false for no")
	}
}
