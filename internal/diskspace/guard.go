// Package diskspace gates capture using available space on the spool filesystem.
// It does not reserve space or replace SQLite's durability/error handling.
package diskspace

import (
	"errors"
	"sync"
	"time"

	"github.com/johnathondillon/write-relay/internal/config"
)

var ErrPaused = errors.New("capture paused by disk-space protection")

// Probe returns bytes available to the daemon's user on the spool filesystem.
// Alternate probes are only injected by tests, never selected by configuration.
type Probe func(path string) (uint64, error)

type Status struct {
	Enabled         bool
	Paused          bool
	Reason          string // Empty, low_disk, or disk_check_failed; never contains OS errors.
	AvailableBytes  uint64
	SampledAt       time.Time // Time of the latest attempt, including a failed probe.
	SampleFresh     bool
	PauseBelowBytes int64
	ResumeAtBytes   int64
}

type Guard struct {
	path     string
	interval time.Duration
	probe    Probe
	now      func() time.Time
	checkMu  sync.Mutex
	mu       sync.RWMutex
	status   Status
	sampleOK bool
}

func New(path string, cfg config.DiskSpaceConfig) *Guard {
	return NewWithProbe(path, cfg, AvailableBytes)
}

// NewWithProbe is for deterministic tests. Production uses New.
func NewWithProbe(path string, cfg config.DiskSpaceConfig, probe Probe) *Guard {
	interval := cfg.CheckInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Guard{path: path, interval: interval, probe: probe, now: time.Now, status: Status{
		Enabled: cfg.PauseBelowBytes > 0, PauseBelowBytes: cfg.PauseBelowBytes, ResumeAtBytes: cfg.ResumeAtBytes,
	}}
}

func (g *Guard) Interval() time.Duration { return g.interval }

// Snapshot never probes the filesystem or waits for an in-progress OS probe.
func (g *Guard) Snapshot() Status {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := g.status
	result.SampleFresh = result.Enabled && g.sampleOK && !result.SampledAt.IsZero() && g.now().Sub(result.SampledAt) <= 2*g.interval
	return result
}

// Check caches routine checks until the interval expires. A forced check always
// probes immediately before admitting a complete batch to SQLite.
func (g *Guard) Check(force bool) error {
	g.checkMu.Lock()
	defer g.checkMu.Unlock()
	g.mu.RLock()
	previous := g.status
	g.mu.RUnlock()
	if !previous.Enabled {
		return nil
	}
	now := g.now()
	if !force && !previous.SampledAt.IsZero() && now.Sub(previous.SampledAt) < g.interval {
		if previous.Paused {
			return ErrPaused
		}
		return nil
	}
	// Do not hold the status lock during statfs: monitoring must remain responsive
	// and expire old samples if an unhealthy filesystem stalls a syscall.
	available, err := g.probe(g.path)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.status.SampledAt = now
	g.sampleOK = err == nil
	if err != nil {
		g.status.Paused = true
		g.status.Reason = "disk_check_failed"
	} else {
		g.status.AvailableBytes = available
		if available < uint64(g.status.PauseBelowBytes) || (previous.Paused && available < uint64(g.status.ResumeAtBytes)) {
			g.status.Paused = true
			g.status.Reason = "low_disk"
		} else {
			g.status.Paused = false
			g.status.Reason = ""
		}
	}
	if g.status.Paused {
		return ErrPaused
	}
	return nil
}
