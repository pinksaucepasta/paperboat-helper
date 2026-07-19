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
  paperboat-helper phase2-harness <absolute-config-path>

The phase2-harness command is for deterministic fake-peer runtime evidence only.
Production runtime bootstrap remains control-plane owned.`

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
	if args[0] == "phase2-harness" {
		if len(args) != 2 {
			writeError(stderr, fmt.Errorf("phase2-harness requires one absolute config path"))
			return 2
		}
		if err := runPhase2Harness(args[1], stdout); err != nil {
			writeError(stderr, err)
			return 1
		}
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
