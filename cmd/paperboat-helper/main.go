package main

import (
	"fmt"
	"io"
	"os"

	"github.com/pinksaucepasta/paperboat-helper/internal/buildinfo"
)

const usage = `paperboat-helper is the Paperboat remote environment runtime.

Usage:
  paperboat-helper version
  paperboat-helper help

Runtime commands will be added with the versioned helper protocol.`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, usage)
		return 0
	}

	if args[0] == "version" || args[0] == "--version" {
		fmt.Fprintf(stdout, "paperboat-helper %s (%s)\n", buildinfo.Version, buildinfo.Commit)
		return 0
	}

	err := fmt.Errorf("unknown command %q", args[0])
	writeError(stderr, err)
	return 2
}

func writeError(w io.Writer, err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(w, "paperboat-helper: %v\n", err)
}
