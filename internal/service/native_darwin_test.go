//go:build darwin

package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	helperruntime "github.com/pinksaucepasta/paperboat-helper/internal/runtime"
)

func TestNativeLaunchdHarnessInstallUpgradeAndUninstall(t *testing.T) {
	if os.Getenv("PAPERBOAT_NATIVE_SERVICE_TEST") != "1" {
		t.Skip("set PAPERBOAT_NATIVE_SERVICE_TEST=1 with a built helper binary")
	}
	executable := os.Getenv("PAPERBOAT_NATIVE_HELPER_BINARY")
	if !filepath.IsAbs(executable) {
		t.Fatal("PAPERBOAT_NATIVE_HELPER_BINARY must be absolute")
	}
	root := t.TempDir()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	definitionPath := filepath.Join(home, "Library", "LaunchAgents", Label+".plist")
	if _, err := os.Lstat(definitionPath); err == nil {
		t.Fatalf("refusing to replace existing service definition %s", definitionPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	public, err := base64.RawURLEncoding.DecodeString("A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg")
	if err != nil {
		t.Fatal(err)
	}
	config := helperruntime.Phase2HarnessConfig{
		Profile: helperconfig.BYOD, StateRoot: filepath.Join(root, "state"), WorkspaceRoot: workspace,
		ListenAddress: "127.0.0.1:18082", ShellPath: "/bin/sh", ShellArgs: []string{"-l"},
		ShellEnvironment: []string{"PATH=/usr/bin:/bin", "TERM=xterm-256color"},
		Issuer:           "https://api.paperboat.test", EnvironmentID: "env_test_01", HelperID: "hlp_test_01",
		PublicKeys: map[string]string{"test-key-1": base64.RawURLEncoding.EncodeToString(public)},
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "phase2-harness.json")
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	installer, err := New(Config{
		Platform: "darwin", ConfigRoot: home, Executable: executable,
		Arguments: []string{"phase2-harness", configPath}, Environment: map[string]string{"HOME": root, "PATH": "/usr/bin:/bin"},
		Controller: LaunchdController{Runner: ExecRunner{}, UID: os.Getuid()},
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupNeeded := false
	t.Cleanup(func() {
		if cleanupNeeded {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			_ = installer.Uninstall(ctx)
		}
	})
	cleanupNeeded = true
	if err := withNativeServiceTimeout(installer.Install); err != nil {
		definition, _ := os.ReadFile(installer.DefinitionPath())
		lint, lintErr := exec.Command("plutil", "-lint", installer.DefinitionPath()).CombinedOutput()
		t.Fatalf("install: %v\nplutil: %s (%v)\ndefinition:\n%s", err, lint, lintErr, definition)
	}
	waitForNativeHealth(t, true)
	if err := withNativeServiceTimeout(installer.Install); err != nil {
		t.Fatal(err)
	}
	waitForNativeHealth(t, true)
	if err := withNativeServiceTimeout(installer.Uninstall); err != nil {
		t.Fatal(err)
	}
	cleanupNeeded = false
	waitForNativeHealth(t, false)
}

func withNativeServiceTimeout(operation func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	return operation(ctx)
}

func waitForNativeHealth(t *testing.T, wantReady bool) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := client.Get("http://127.0.0.1:18082/healthz")
		ready := err == nil && response.StatusCode == http.StatusOK
		if response != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
			response.Body.Close()
		}
		if ready == wantReady {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("health ready=%v want=%v err=%v", ready, wantReady, err)
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-time.After(time.Until(deadline)):
			t.Fatalf("health ready=%v want=%v", ready, wantReady)
		}
	}
}
