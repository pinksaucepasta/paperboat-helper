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

func TestPhase2HarnessRequiresExactlyOneConfigPath(t *testing.T) {
	for _, arguments := range [][]string{{"phase2-harness"}, {"phase2-harness", "/one", "/two"}} {
		var stdout, stderr bytes.Buffer
		if code := run(arguments, &stdout, &stderr); code != 2 {
			t.Fatalf("arguments=%v code=%d stderr=%q", arguments, code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "requires one absolute config path") {
			t.Fatalf("stderr=%q", stderr.String())
		}
	}
}

func TestHelpLabelsPhase2HarnessAsFakePeerOnly(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "phase2-harness") || !strings.Contains(stdout.String(), "fake-peer runtime evidence only") || strings.Contains(stdout.String(), "paperboat-helper run") {
		t.Fatalf("help=%q", stdout.String())
	}
}
