package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pinksaucepasta/paperboat-helper/internal/buildinfo"
)

const usage = `pbh is the Paperboat remote environment runtime.

Usage:
  pbh version
  pbh help
  pbh bootstrap --server <url> --enrollment-token <token> --name <name> [--shell <absolute-path>]
  pbh preview create --name <name> --port <port> --public
  pbh preview list
  pbh preview remove <name>
  pbh doctor [--json]
  pbh service uninstall
  pbh run

The bootstrap command performs dashboard-started enrollment, verifies the signed helper
artifact, requests administrator approval for the durable system service, and waits for readiness.`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	return runWithInput(args, os.Stdin, stdout, stderr)
}

func runWithInput(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, usage)
		return 0
	}

	if args[0] == "version" || args[0] == "--version" || args[0] == "-v" {
		fmt.Fprintf(stdout, "pbh %s (%s)\n", buildinfo.Version, buildinfo.Commit)
		return 0
	}
	if args[0] == "bootstrap" {
		if err := runBootstrap(context.Background(), args[1:], stdin, stdout, stderr); err != nil {
			writeError(stderr, err)
			return 1
		}
		return 0
	}
	if args[0] == "service" {
		if err := runServiceCommand(context.Background(), args[1:], stdin, stdout, stderr); err != nil {
			writeError(stderr, err)
			return 1
		}
		return 0
	}
	if args[0] == "doctor" {
		if err := runDoctor(context.Background(), args[1:], stdout, stderr); err != nil {
			writeError(stderr, err)
			return 1
		}
		return 0
	}
	if args[0] == "preview" {
		if err := runPreview(context.Background(), args[1:], stdout, stderr); err != nil {
			writeError(stderr, err)
			return 1
		}
		return 0
	}
	if args[0] == "run" {
		if len(args) != 1 {
			writeError(stderr, fmt.Errorf("run does not accept arguments"))
			return 2
		}
		if err := runProduction(stdout); err != nil {
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
	fmt.Fprintf(w, "pbh: %v\n", err)
}
