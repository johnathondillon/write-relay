package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

const usage = `usage: writerelayd <command> [options]

commands:
  run       capture committed PostgreSQL events
  doctor    inspect configuration and dependencies
  setup     install database objects and optionally create a slot
  spool     inspect the local durable spool
  version   print build version
`

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

func Execute(ctx context.Context, args []string, stdout, stderr io.Writer, build BuildInfo) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return errors.New("command is required")
	}

	switch args[0] {
	case "version":
		return versionCommand(args[1:], stdout, build)
	case "run":
		return runCommand(ctx, args[1:], stdout, stderr)
	case "doctor":
		return doctorCommand(ctx, args[1:], stdout, stderr)
	case "setup":
		return setupCommand(ctx, args[1:], stdout, stderr)
	case "spool":
		return spoolCommand(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	default:
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func configFlagSet(name string, stderr io.Writer) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config", "./writerelay.yaml", "path to configuration YAML")
	return fs, path
}
