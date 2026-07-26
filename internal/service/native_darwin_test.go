//go:build darwin

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const nativeDarwinHealthURL = "http://127.0.0.1:18082/healthz"

func TestNativeLaunchdServiceProcess(t *testing.T) {
	if os.Getenv("PAPERBOAT_NATIVE_SERVICE_CHILD") != "1" {
		t.Skip("native service child only")
	}
	server := &http.Server{Addr: "127.0.0.1:18082", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestNativeLaunchdInstallUpgradeAndUninstall(t *testing.T) {
	if os.Getenv("PAPERBOAT_NATIVE_SERVICE_TEST") != "1" {
		t.Skip("set PAPERBOAT_NATIVE_SERVICE_TEST=1 in a logged-in macOS user session")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	definitionPath := filepath.Join("/Library", "LaunchDaemons", Label+".plist")
	if _, err := os.Lstat(definitionPath); err == nil {
		t.Fatalf("refusing to replace existing service definition %s", definitionPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	installer, err := New(Config{
		Platform: "darwin", ConfigRoot: "/", Executable: executable, User: os.Getenv("USER"), Group: "staff",
		Arguments:   []string{"-test.run=^TestNativeLaunchdServiceProcess$", "-test.v"},
		Environment: map[string]string{"PAPERBOAT_NATIVE_SERVICE_CHILD": "1"},
		Controller:  LaunchdController{Runner: ExecRunner{}, UID: os.Getuid()},
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupNeeded := true
	t.Cleanup(func() {
		if cleanupNeeded {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			_ = installer.Uninstall(ctx)
		}
	})
	if err := nativeLaunchdOperation(installer.Install); err != nil {
		definition, _ := os.ReadFile(installer.DefinitionPath())
		t.Fatalf("install: %v\ndefinition:\n%s", err, definition)
	}
	waitForNativeLaunchdHealth(t, true)
	if err := nativeLaunchdOperation(installer.Install); err != nil {
		t.Fatal(err)
	}
	waitForNativeLaunchdHealth(t, true)
	if err := nativeLaunchdOperation(installer.Uninstall); err != nil {
		t.Fatal(err)
	}
	cleanupNeeded = false
	waitForNativeLaunchdHealth(t, false)
}

func nativeLaunchdOperation(operation func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	return operation(ctx)
}

func waitForNativeLaunchdHealth(t *testing.T, wantReady bool) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := client.Get(nativeDarwinHealthURL)
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
		time.Sleep(50 * time.Millisecond)
	}
}
