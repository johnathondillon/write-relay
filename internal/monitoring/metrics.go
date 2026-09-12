package monitoring

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

// This small fixed set uses Prometheus text exposition 0.0.4. Labels are limited
// to durable sink names and fixed categories; event identities never appear.
func writeMetrics(w io.Writer, capture postgres.CaptureStatus, stats sqlitespool.Stats, fresh bool, now time.Time) {
	disk := capture.Disk
	gauge(w, "capture_paused", "Capture is paused by disk-space protection.", boolNumber(disk.Paused))
	header(w, "capture_pause", "Capture pause reason; fixed categories.", "gauge")
	for _, reason := range []string{"low_disk", "disk_check_failed"} {
		fmt.Fprintf(w, "writerelay_capture_pause{reason=%s} %d\n", label(reason), boolNumber(disk.Paused && disk.Reason == reason))
	}
	gauge(w, "disk_protection_enabled", "Whether capture disk-space protection is enabled.", boolNumber(disk.Enabled))
	if disk.Enabled {
		gauge(w, "disk_sample_fresh", "Latest disk-space probe succeeded and is fresh.", boolNumber(disk.SampleFresh))
		var attempted int64
		if !disk.SampledAt.IsZero() {
			attempted = disk.SampledAt.Unix()
		}
		gauge(w, "disk_sample_timestamp_seconds", "Unix time of the latest disk-space probe attempt, or zero.", attempted)
		gauge(w, "disk_pause_below_bytes", "Capture pauses below this available-byte threshold.", disk.PauseBelowBytes)
		gauge(w, "disk_resume_at_bytes", "Paused capture resumes at this available-byte threshold.", disk.ResumeAtBytes)
		if disk.SampleFresh {
			header(w, "disk_available_bytes", "Bytes available to this user on the spool filesystem; not reserved space.", "gauge")
			fmt.Fprintf(w, "writerelay_disk_available_bytes %d\n", disk.AvailableBytes)
		}
	}
	gauge(w, "capture_connected", "Replication stream started and has not observed a disconnect.", boolNumber(capture.Connected))
	header(w, "capture_transactions_total", "Transactions persisted and status updates sent by this process, including empty batches and replays.", "counter")
	fmt.Fprintf(w, "writerelay_capture_transactions_total %d\n", capture.Transactions)
	gauge(w, "capture_last_success_timestamp_seconds", "Unix time of last persisted batch and sent status update in this process, or zero.", capture.LastCaptureUnix)
	gauge(w, "spool_sample_fresh", "One when the latest spool sample succeeded and is within the freshness limit.", boolNumber(fresh))
	var timestamp int64
	if !stats.SampledAt.IsZero() {
		timestamp = stats.SampledAt.Unix()
	}
	gauge(w, "spool_sample_timestamp_seconds", "Unix time of the last successful spool snapshot, or zero.", timestamp)
	if !fresh {
		return // Do not turn a failed read into zero backlog or export stale counts.
	}
	gauge(w, "spool_events", "Retained event identities, including pruned payloads.", stats.EventCount)
	header(w, "spool_payloads", "Event payload counts by retention state.", "gauge")
	fmt.Fprintf(w, "writerelay_spool_payloads{state=\"retained\"} %d\nwriterelay_spool_payloads{state=\"pruned\"} %d\n", stats.EventCount-stats.PrunedPayloads, stats.PrunedPayloads)
	header(w, "spool_file_bytes", "Approximate SQLite file lengths; not allocated blocks or PostgreSQL WAL.", "gauge")
	fmt.Fprintf(w, "writerelay_spool_file_bytes{file=\"database\"} %d\nwriterelay_spool_file_bytes{file=\"wal\"} %d\nwriterelay_spool_file_bytes{file=\"shm\"} %d\n", stats.Storage.DatabaseBytes, stats.Storage.WALBytes, stats.Storage.SHMBytes)
	header(w, "sink_active", "Whether the registered sink is currently configured.", "gauge")
	for _, sink := range stats.Sinks {
		fmt.Fprintf(w, "writerelay_sink_active{sink=%s} %d\n", label(sink.Name), boolNumber(sink.Active))
	}
	header(w, "deliveries", "Durable delivery records by sink and state, including inactive sinks.", "gauge")
	for _, sink := range stats.Sinks {
		counts := sink.Deliveries
		for _, state := range []struct {
			name  string
			count int64
		}{{"pending", counts.Pending}, {"retry_wait", counts.RetryWait}, {"delivered", counts.Delivered}, {"dead_letter", counts.DeadLetter}} {
			fmt.Fprintf(w, "writerelay_deliveries{sink=%s,state=%s} %d\n", label(sink.Name), label(state.name), state.count)
		}
	}
	header(w, "oldest_waiting_age_seconds", "Age since original capture of the oldest pending or retry_wait event in the snapshot, or zero.", "gauge")
	for _, sink := range stats.Sinks {
		var age float64
		if sink.OldestWaiting != nil {
			age = max(0, now.Sub(sink.OldestWaiting.CapturedAt).Seconds())
		}
		fmt.Fprintf(w, "writerelay_oldest_waiting_age_seconds{sink=%s} %g\n", label(sink.Name), age)
	}
}

func header(w io.Writer, name, help, kind string) {
	fmt.Fprintf(w, "# HELP writerelay_%s %s\n# TYPE writerelay_%s %s\n", name, help, name, kind)
}

func gauge(w io.Writer, name, help string, value int64) {
	header(w, name, help, "gauge")
	fmt.Fprintf(w, "writerelay_%s %d\n", name, value)
}

var labelEscaper = strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"")

func label(value string) string { return "\"" + labelEscaper.Replace(value) + "\"" }

func boolNumber(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
