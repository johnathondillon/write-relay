package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func doctorCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, configPath := configFlagSet("doctor", stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("doctor does not accept positional arguments")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	checks, err := postgres.Doctor(ctx, cfg)
	if err != nil {
		return err
	}
	checks = append(checks, spoolChecks(ctx, cfg)...)
	checks = append(checks, deliveryChecks(cfg)...)
	failed := false
	for _, check := range checks {
		fmt.Fprintf(stdout, "%-5s %-30s %s\n", check.Status, check.Name, check.Detail)
		if check.Status == "fail" {
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("one or more doctor checks failed")
	}
	return nil
}

func deliveryChecks(cfg config.Config) []postgres.Check {
	if len(cfg.Delivery.Sinks) == 0 {
		return []postgres.Check{{
			Name: "delivery sinks", Status: "pass", Detail: "capture-only mode",
		}}
	}
	checks := make([]postgres.Check, 0, len(cfg.Delivery.Sinks))
	for _, sink := range cfg.Delivery.Sinks {
		if sink.Type == "webhook" {
			_, _, err := delivery.NewWebhookSender(sink, cfg.Delivery.RequestTimeout)
			if err != nil {
				checks = append(checks, postgres.Check{
					Name: "delivery " + sink.Name, Status: "fail", Detail: err.Error(),
				})
				continue
			}
		}
		checks = append(checks, postgres.Check{
			Name: "delivery " + sink.Name, Status: "pass",
			Detail: "configuration and secret references are valid",
		})
	}
	return checks
}

func spoolChecks(ctx context.Context, cfg config.Config) []postgres.Check {
	if _, err := os.Stat(cfg.Spool.Path); err == nil {
		store, openErr := sqlitespool.Open(ctx, cfg.Spool.Path)
		if openErr != nil {
			return []postgres.Check{{Name: "spool database", Status: "fail", Detail: openErr.Error()}}
		}
		defer store.Close()
		version, versionErr := store.SchemaVersion(ctx)
		if versionErr != nil {
			return []postgres.Check{{Name: "spool schema", Status: "fail", Detail: versionErr.Error()}}
		}
		return []postgres.Check{{Name: "spool schema", Status: "pass", Detail: fmt.Sprintf("version %d", version)}}
	} else if !os.IsNotExist(err) {
		return []postgres.Check{{Name: "spool path", Status: "fail", Detail: err.Error()}}
	}

	parent := filepath.Dir(cfg.Spool.Path)
	for {
		info, err := os.Stat(parent)
		if err == nil {
			if !info.IsDir() {
				return []postgres.Check{{Name: "spool path", Status: "fail", Detail: "parent is not a directory"}}
			}
			if info.Mode().Perm()&0o200 == 0 {
				return []postgres.Check{{Name: "spool path", Status: "fail", Detail: "parent is not writable"}}
			}
			return []postgres.Check{{Name: "spool path", Status: "pass", Detail: "new spool can be created"}}
		}
		next := filepath.Dir(parent)
		if next == parent {
			return []postgres.Check{{Name: "spool path", Status: "fail", Detail: "no existing parent directory"}}
		}
		parent = next
	}
}
