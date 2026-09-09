package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/johnathondillon/write-relay/internal/config"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func spoolStatsCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, configPath := configFlagSet("spool stats", stderr)
	jsonOutput := fs.Bool("json", false, "print a JSON snapshot instead of a readable summary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("spool stats does not accept positional arguments")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	stats, err := sqlitespool.ReadStats(ctx, cfg.Spool.Path)
	if err != nil {
		return err
	}
	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(stats); err != nil {
			return fmt.Errorf("write spool stats: %w", err)
		}
		return nil
	}
	return writeStatsSummary(stdout, stats)
}

func writeStatsSummary(stdout io.Writer, stats sqlitespool.Stats) error {
	var output bytes.Buffer
	fmt.Fprintf(&output, "Spool snapshot: %s\n", stats.SampledAt.Format(time.RFC3339))
	fmt.Fprintf(&output, "Captured events: %d\n", stats.EventCount)
	fmt.Fprintf(&output, "Durable checkpoint: %s\n", stats.LastDurableLSN)
	fmt.Fprintf(&output, "Deliveries: %d total | %d pending | %d retry_wait | %d delivered | %d dead_letter\n",
		stats.Deliveries.Total, stats.Deliveries.Pending, stats.Deliveries.RetryWait,
		stats.Deliveries.Delivered, stats.Deliveries.DeadLetter)
	fmt.Fprintf(&output, "Oldest waiting: %s\n", waitingAge(stats.OldestWaiting))
	fmt.Fprintf(&output, "Spool files: %s total (database %s, SQLite WAL %s, SHM %s)\n\n",
		fileSize(stats.Storage.TotalBytes), fileSize(stats.Storage.DatabaseBytes),
		fileSize(stats.Storage.WALBytes), fileSize(stats.Storage.SHMBytes))
	if len(stats.Sinks) == 0 {
		fmt.Fprintln(&output, "No registered sinks.")
	} else {
		table := tabwriter.NewWriter(&output, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "SINK\tTYPE\tACTIVE\tPENDING\tRETRY_WAIT\tDELIVERED\tDEAD_LETTER\tOLDEST WAITING")
		for _, sink := range stats.Sinks {
			fmt.Fprintf(table, "%s\t%s\t%t\t%d\t%d\t%d\t%d\t%s\n",
				sink.Name, sink.Type, sink.Active, sink.Deliveries.Pending, sink.Deliveries.RetryWait,
				sink.Deliveries.Delivered, sink.Deliveries.DeadLetter, waitingAge(sink.OldestWaiting))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("format spool stats: %w", err)
		}
	}
	fmt.Fprintln(&output, "\nWaiting = pending + retry_wait; age is since original local capture.")
	fmt.Fprintln(&output, "File sizes are approximate. This snapshot does not establish daemon or destination health.")
	if _, err := output.WriteTo(stdout); err != nil {
		return fmt.Errorf("write spool stats: %w", err)
	}
	return nil
}

func waitingAge(waiting *sqlitespool.WaitingStats) string {
	if waiting == nil {
		return "none"
	}
	return (time.Duration(waiting.AgeSeconds) * time.Second).String()
}

func fileSize(size int64) string {
	const kib = 1024
	if size < kib {
		return fmt.Sprintf("%d B", size)
	}
	value := float64(size)
	for _, unit := range []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"} {
		value /= kib
		if value < kib || unit == "EiB" {
			return fmt.Sprintf("%.1f %s", value, unit)
		}
	}
	return ""
}
