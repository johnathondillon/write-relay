package cli

import (
	"flag"
	"fmt"
	"io"
)

func versionCommand(args []string, stdout io.Writer, build BuildInfo) error {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(stdout)
	if err := fs.Parse(args); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "writerelayd %s commit=%s built=%s\n", build.Version, build.Commit, build.Date)
	return nil
}
