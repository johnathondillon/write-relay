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

func spoolCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: writerelayd spool <list|deliveries|redrive> [options]")
	}
	switch args[0] {
	case "list":
		return spoolListCommand(ctx, args[1:], stdout, stderr)
	case "deliveries":
		return spoolDeliveriesCommand(ctx, args[1:], stdout, stderr)
	case "redrive":
		return spoolRedriveCommand(ctx, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown spool command %q", args[0])
	}
}

func spoolListCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, configPath := configFlagSet("spool list", stderr)
	limit := fs.Int("limit", 20, "maximum rows to print (1-1000)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("spool list does not accept positional arguments")
	}
	store, err := openConfiguredSpool(ctx, *configPath)
	if err != nil {
		return err
	}
	defer store.Close()
	rows, err := store.ListEvents(ctx, *limit)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	for _, row := range rows {
		output := struct {
			Sequence      int64           `json:"sequence"`
			Source        string          `json:"source"`
			ID            string          `json:"id"`
			Type          string          `json:"type"`
			Subject       string          `json:"subject,omitempty"`
			Payload       json.RawMessage `json:"payload"`
			TransactionID uint32          `json:"transaction_id"`
			MessageIndex  int             `json:"message_index"`
			CommitEndLSN  string          `json:"commit_end_lsn"`
		}{
			Sequence: row.Sequence, Source: row.Source, ID: row.ID, Type: row.Type,
			Subject: row.Subject, Payload: row.Payload, TransactionID: row.TransactionID,
			MessageIndex: row.MessageIndex, CommitEndLSN: row.CommitEndLSN,
		}
		if err := encoder.Encode(output); err != nil {
			return fmt.Errorf("write spool row: %w", err)
		}
	}
	return nil
}

func spoolDeliveriesCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, configPath := configFlagSet("spool deliveries", stderr)
	limit := fs.Int("limit", 20, "maximum rows to print (1-1000)")
	state := fs.String("state", "", "optional delivery state filter")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("spool deliveries does not accept positional arguments")
	}
	store, err := openConfiguredSpool(ctx, *configPath)
	if err != nil {
		return err
	}
	defer store.Close()
	rows, err := store.ListDeliveries(ctx, *state, *limit)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("write delivery row: %w", err)
		}
	}
	return nil
}

func spoolRedriveCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, configPath := configFlagSet("spool redrive", stderr)
	sink := fs.String("sink", "", "sink name")
	source := fs.String("source", "", "event source")
	id := fs.String("id", "", "event id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("spool redrive does not accept positional arguments")
	}
	if *sink == "" || *source == "" || *id == "" {
		return fmt.Errorf("spool redrive requires --sink, --source, and --id")
	}
	store, err := openConfiguredSpool(ctx, *configPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.RedriveDelivery(ctx, *sink, *source, *id, time.Now().UTC()); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "redrive scheduled for sink=%q source=%q id=%q\n", *sink, *source, *id)
	return nil
}

func openConfiguredSpool(ctx context.Context, path string) (*sqlitespool.Store, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	return sqlitespool.Open(ctx, cfg.Spool.Path)
}
