//go:build darwin || linux

package pty

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func shellPath(t *testing.T) string {
	t.Helper()
	for _, path := range []string{"/bin/sh", "/usr/bin/sh"} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	t.Fatal("no test shell")
	return ""
}
func startShell(t *testing.T, script string) (*Process, string) {
	t.Helper()
	root := t.TempDir()
	adapter, err := NewAdapter(root)
	if err != nil {
		t.Fatal(err)
	}
	process, err := adapter.Start(Command{Path: shellPath(t), Args: []string{"-c", script}, Env: []string{"PATH=/usr/bin:/bin", "TERM=xterm"}, CWD: root, Dimensions: Dimensions{80, 24}})
	if err != nil {
		t.Fatal(err)
	}
	return process, root
}

func TestRealPTYReportsOutputCWDAndExit(t *testing.T) {
	process, root := startShell(t, "pwd; printf hello; exit 7")
	output, readErr := io.ReadAll(process)
	if readErr != nil {
		t.Fatal(readErr)
	}
	result, err := process.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer process.CloseIO()
	if result.Code != 7 || !strings.Contains(string(output), root) || !strings.Contains(string(output), "hello") {
		t.Fatalf("result=%#v output=%q", result, output)
	}
}

func TestRealPTYInitialSizeAndResize(t *testing.T) {
	process, _ := startShell(t, "stty size; echo resize-ready; read line; stty size")
	reader := bufio.NewReader(process)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	marker, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(marker, "resize-ready") {
		t.Fatalf("marker=%q err=%v", marker, err)
	}
	if err := process.Resize(Dimensions{100, 30}); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Write([]byte("\n")); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	result, err := process.Wait(context.Background())
	if err != nil || result.Code != 0 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	defer process.CloseIO()
	text := first + string(output)
	if !strings.Contains(text, "24 80") || !strings.Contains(text, "30 100") {
		t.Fatalf("output=%q", text)
	}
}

func TestSignalTargetsPTYProcessGroup(t *testing.T) {
	process, _ := startShell(t, "trap 'exit 42' TERM; echo ready; while :; do sleep 1; done")
	buffer := make([]byte, 64)
	if _, err := process.Read(buffer); err != nil {
		t.Fatal(err)
	}
	if err := process.Signal(Terminate); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := process.Wait(ctx)
	defer process.CloseIO()
	if err != nil || result.Code != 42 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAdapterRejectsEscapedCWDAndInvalidEnvironment(t *testing.T) {
	root := t.TempDir()
	adapter, err := NewAdapter(root)
	if err != nil {
		t.Fatal(err)
	}
	command := Command{Path: shellPath(t), Args: []string{"-c", "exit"}, Env: []string{"PATH=/bin"}, CWD: filepath.Dir(root), Dimensions: Dimensions{80, 24}}
	if _, err := adapter.Start(command); !errors.Is(err, ErrInvalidCWD) {
		t.Fatalf("cwd err=%v", err)
	}
	command.CWD = root
	command.Env = []string{"A=1", "A=2"}
	if _, err := adapter.Start(command); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("env err=%v", err)
	}
}

func TestProcessPolicyRejectsUnboundedOrNULStaticValues(t *testing.T) {
	tooManyArguments := make([]string, 65)
	tooManyEnvironment := make([]string, 129)
	for index := range tooManyEnvironment {
		tooManyEnvironment[index] = fmt.Sprintf("KEY_%d=value", index)
	}
	for _, test := range []struct {
		arguments   []string
		environment []string
	}{
		{arguments: []string{"bad\x00argument"}},
		{arguments: tooManyArguments},
		{environment: []string{"BAD=bad\x00value"}},
		{environment: tooManyEnvironment},
	} {
		if _, err := ValidateProcessPolicy("/bin/sh", test.arguments, test.environment); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("arguments=%d environment=%d err=%v", len(test.arguments), len(test.environment), err)
		}
	}
}

func TestTerminateEscalatesWithinBound(t *testing.T) {
	process, _ := startShell(t, "trap '' TERM; while :; do sleep 1; done")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := process.Terminate(ctx, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if result.Signal == "" && result.Code == 0 {
		t.Fatalf("result=%#v", result)
	}
}
