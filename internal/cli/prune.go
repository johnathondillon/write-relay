package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/johnathondillon/write-relay/internal/config"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func spoolPruneCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, configPath := configFlagSet("spool prune", stderr)
	before := fs.String("before", "", "required RFC 3339 cutoff; every delivery must have succeeded before it")
	limit := fs.Int("limit", 1000, "maximum payloads to prune in one transaction (1-1000)")
	dryRun := fs.Bool("dry-run", false, "preview eligible payloads without modifying the spool")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("spool prune does not accept positional arguments")
	}
	cutoff, err := time.Parse(time.RFC3339Nano, *before)
	if err != nil {
		return fmt.Errorf("spool prune requires --before with an RFC 3339 timestamp")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	result, err := sqlitespool.Prune(ctx, cfg.Spool.Path, sqlitespool.PruneOptions{Before: cutoff, Limit: *limit, DryRun: *dryRun})
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		if *dryRun {
			return fmt.Errorf("write prune preview: %w", err)
		}
		return fmt.Errorf("write prune result (cleanup already committed; rerun dry-run to inspect): %w", err)
	}
	return nil
}
