package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/logging"
	"github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func runCommand(ctx context.Context, args []string, _ io.Writer, stderr io.Writer) error {
	fs, configPath := configFlagSet("run", stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("run does not accept positional arguments")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	logger, err := logging.New(cfg.Logging, os.Stderr)
	if err != nil {
		return err
	}
	store, err := sqlitespool.Open(ctx, cfg.Spool.Path)
	if err != nil {
		return err
	}
	defer store.Close()
	return postgres.NewReplicator(cfg, store, logger).Run(ctx)
}
