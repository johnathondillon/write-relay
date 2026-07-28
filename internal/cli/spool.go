package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/johnathondillon/write-relay/internal/config"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func spoolCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "list" {
		return fmt.Errorf("usage: writerelayd spool list --config PATH --limit N")
	}
	fs, configPath := configFlagSet("spool list", stderr)
	limit := fs.Int("limit", 20, "maximum rows to print (1-1000)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("spool list does not accept positional arguments")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	store, err := sqlitespool.Open(ctx, cfg.Spool.Path)
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
