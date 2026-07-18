package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run version exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "paperboat-helper ") {
		t.Fatalf("version output = %q", stdout.String())
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve"}, &stdout, &stderr); code != 2 {
		t.Fatalf("run unknown exit code = %d, want 2", code)
	}
	if got := stderr.String(); got != "paperboat-helper: unknown command \"serve\"\n" {
		t.Fatalf("stderr = %q", got)
	}
}
