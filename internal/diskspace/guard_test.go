package diskspace

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/johnathondillon/write-relay/internal/config"
)

func TestGuardThresholdsCachingErrorsAndRecovery(t *testing.T) {
	now := time.Now()
	var free uint64 = 100
	var probeErr error
	calls := 0
	g := NewWithProbe("the-spool", config.DiskSpaceConfig{PauseBelowBytes: 100, ResumeAtBytes: 200, CheckInterval: time.Second}, func(path string) (uint64, error) {
		if path != "the-spool" {
			t.Fatal(path)
		}
		calls++
		return free, probeErr
	})
	g.now = func() time.Time { return now }
	if g.Snapshot().SampleFresh {
		t.Fatal("no initial sample")
	}
	if g.Check(false) != nil {
		t.Fatal("threshold equality must allow initial capture")
	}
	free = 99
	if g.Check(false) != nil || calls != 1 {
		t.Fatal("routine check must use fresh cache")
	}
	if !errors.Is(g.Check(true), ErrPaused) || g.Snapshot().Reason != "low_disk" {
		t.Fatal(g.Snapshot())
	}
	free = 150
	now = now.Add(time.Second)
	if !errors.Is(g.Check(false), ErrPaused) {
		t.Fatal("resumed below recovery threshold")
	}
	free = 200
	if g.Check(true) != nil || g.Snapshot().Paused {
		t.Fatal("resume boundary")
	}
	probeErr = errors.New("PRIVATE filesystem path")
	if !errors.Is(g.Check(true), ErrPaused) || g.Snapshot().Reason != "disk_check_failed" || g.Snapshot().SampleFresh {
		t.Fatal(g.Snapshot())
	}
	probeErr = nil
	free = 150
	if g.Check(true) == nil {
		t.Fatal("failed measurement also requires recovery threshold")
	}
	free = 200
	if g.Check(true) != nil || !g.Snapshot().SampleFresh {
		t.Fatal(g.Snapshot())
	}
	now = now.Add(3 * time.Second)
	if g.Snapshot().SampleFresh {
		t.Fatal("stale available bytes")
	}
}

func TestDisabledGuardNeverProbes(t *testing.T) {
	g := NewWithProbe("missing", config.DiskSpaceConfig{}, func(string) (uint64, error) { t.Fatal("disabled guard probed"); return 0, nil })
	if g.Check(true) != nil || g.Snapshot().Enabled || g.Snapshot().Paused {
		t.Fatal(g.Snapshot())
	}
}

func TestSnapshotRemainsResponsiveDuringBlockedProbe(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	release := sync.OnceFunc(func() { close(finish) })
	defer release()
	g := NewWithProbe("spool", config.DiskSpaceConfig{PauseBelowBytes: 100, ResumeAtBytes: 200}, func(string) (uint64, error) { close(entered); <-finish; return 1000, nil })
	go func() { _ = g.Check(true); close(done) }()
	<-entered
	read := make(chan Status, 1)
	go func() { read <- g.Snapshot() }()
	select {
	case s := <-read:
		if s.SampleFresh {
			t.Fatal(s)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot blocked on filesystem probe")
	}
	release()
	<-done
}
