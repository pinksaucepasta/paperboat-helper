//go:build darwin

package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/service"
)

const nativeBootstrapDarwinHealthURL = "http://127.0.0.1:18085/healthz"

func TestNativeBootstrapLaunchdServiceProcess(t *testing.T) {
	if os.Getenv("PAPERBOAT_NATIVE_BOOTSTRAP_DARWIN_CHILD") != "1" {
		t.Skip("native bootstrap child only")
	}
	server := &http.Server{Addr: "127.0.0.1:18085", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestNativeBootstrapLaunchdFailureRetryAndCleanup(t *testing.T) {
	if os.Getenv("PAPERBOAT_NATIVE_BOOTSTRAP_DARWIN_TEST") != "1" {
		t.Skip("set PAPERBOAT_NATIVE_BOOTSTRAP_DARWIN_TEST=1 in a logged-in macOS user session")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() != 0 {
		t.Fatal("native LaunchDaemon test must run as root with an isolated test account")
	}
	definition := filepath.Join("/Library", "LaunchDaemons", service.Label+".plist")
	if _, err := os.Lstat(definition); err == nil {
		t.Fatalf("refusing to replace existing service definition %s", definition)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	runner := &darwinFailFirstRunner{next: service.ExecRunner{}}
	installer, err := service.New(service.Config{
		Platform: "darwin", ConfigRoot: "/", Executable: executable, User: "nobody", Group: "nobody",
		Arguments:   []string{"-test.run=^TestNativeBootstrapLaunchdServiceProcess$", "-test.v"},
		Environment: map[string]string{"PAPERBOAT_NATIVE_BOOTSTRAP_DARWIN_CHILD": "1"},
		Controller:  service.LaunchdController{Runner: runner, UID: os.Getuid()},
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
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := installService(ctx, installer, 3, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if runner.calls < 2 {
		t.Fatalf("launchd manager calls=%d", runner.calls)
	}
	waitForBootstrapDarwinHealth(t, true)
	if err := installer.Uninstall(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupNeeded = false
	waitForBootstrapDarwinHealth(t, false)
}

type darwinFailFirstRunner struct {
	mu    sync.Mutex
	calls int
	next  service.Runner
}

func (r *darwinFailFirstRunner) Run(ctx context.Context, name string, args ...string) error {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 1 {
		return errors.New("injected launchd manager failure")
	}
	return r.next.Run(ctx, name, args...)
}

func waitForBootstrapDarwinHealth(t *testing.T, wantReady bool) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := client.Get(nativeBootstrapDarwinHealthURL)
		ready := err == nil && response.StatusCode == http.StatusOK
		if response != nil {
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
