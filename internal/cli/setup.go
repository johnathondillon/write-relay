package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/postgres"
)

func setupCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, configPath := configFlagSet("setup", stderr)
	createSlot := fs.Bool("create-slot", false, "create a missing pgoutput replication slot")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("setup does not accept positional arguments")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	result, err := postgres.Setup(ctx, cfg, *createSlot)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "setup complete: installed_function=%t created_publication=%t created_slot=%t\n",
		result.InstalledFunction, result.CreatedPublication, result.CreatedSlot)
	return nil
}
