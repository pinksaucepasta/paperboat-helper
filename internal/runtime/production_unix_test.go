//go:build darwin || linux

package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/connector"
)

type testTokenSource struct{}

func (testTokenSource) Token(context.Context) (string, error) { return "helper-identity", nil }

type testProofSource struct{ body []byte }

func (p *testProofSource) Proof(_ context.Context, _ string, method, path string, body []byte) ([]byte, error) {
	if method != http.MethodPost || path != "/v1/runtime-observations" {
		return nil, errors.New("wrong proof target")
	}
	p.body = append([]byte(nil), body...)
	return []byte("proof"), nil
}

func TestRuntimeObservationUsesRenewableIdentityAndExactBodyProof(t *testing.T) {
	var gotAuth, gotProof string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := new(bytes.Buffer)
		_, _ = body.ReadFrom(r.Body)
		gotAuth, gotProof = r.Header.Get("Authorization"), r.Header.Get("X-Paperboat-Helper-Proof")
		if r.URL.Path != "/v1/runtime-observations" || !strings.Contains(body.String(), `"environment_id":"prj_1"`) {
			t.Errorf("request path/body = %s %s", r.URL.Path, body.String())
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	proofs := &testProofSource{}
	sender := &runtimeObservationSender{endpoint: server.URL + "/v1/runtime-observations", tokens: testTokenSource{}, proofs: proofs, operationID: func() (string, error) { return "op-1", nil }, environmentID: "prj_1", machineID: "mach_1", reporterVersion: "test", client: server.Client()}
	if err := sender.Send(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer helper-identity" || gotProof != base64.RawURLEncoding.EncodeToString([]byte("proof")) {
		t.Fatalf("headers auth=%q proof=%q", gotAuth, gotProof)
	}
	if len(proofs.body) == 0 || !bytes.Contains(proofs.body, []byte(`"resource_id":"mach_1"`)) {
		t.Fatalf("proof body=%s", proofs.body)
	}
}

func TestProductionHelperRequiresHTTPSControl(t *testing.T) {
	base := map[string]string{"PAPERBOAT_HELPER_STATE_ROOT": filepath.Join(t.TempDir(), "state")}
	base["PAPERBOAT_HELPER_PROFILE"] = "byod"
	base["PAPERBOAT_WORKSPACE_ROOT"] = t.TempDir()
	base["PAPERBOAT_CONTROL_URL"] = "http://control.example.test"
	base["PAPERBOAT_USER_MACHINE_ID"] = "um_1"
	if _, err := NewProductionHelper(context.Background(), "test", func(name string) string { return base[name] }); !errors.Is(err, ErrProductionInvalid) {
		t.Fatalf("byod control error=%v", err)
	}
	base["PAPERBOAT_HELPER_PROFILE"] = "hosted"
	base["PAPERBOAT_WORKSPACE"] = filepath.Join(t.TempDir(), "volume")
	base["PAPERBOAT_PROJECT_ID"] = "prj_1"
	base["PAPERBOAT_REPOSITORY_URL"] = "https://github.com/paperboat/example.git"
	base["PAPERBOAT_CONTROL_URL"] = "http://control.example.test"
	if _, err := NewProductionHelper(context.Background(), "test", func(name string) string { return base[name] }); !errors.Is(err, ErrProductionInvalid) {
		t.Fatalf("control error=%v", err)
	}
}

func TestValidatedBYODShellRequiresExecutableAbsoluteFile(t *testing.T) {
	shell := filepath.Join(t.TempDir(), "shell")
	if err := os.WriteFile(shell, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got, err := validatedBYODShell(shell); err != nil || got != shell {
		t.Fatalf("shell=%q err=%v", got, err)
	}
	for _, invalid := range []string{"relative", filepath.Join(t.TempDir(), "missing")} {
		if _, err := validatedBYODShell(invalid); !errors.Is(err, ErrProductionInvalid) {
			t.Fatalf("invalid shell %q err=%v", invalid, err)
		}
	}
	if got, err := validatedBYODShell(""); err != nil || got != "/bin/sh" {
		t.Fatalf("fallback shell=%q err=%v", got, err)
	}
}

func TestValidateBYODWorkspaceRejectsNonCanonicalAndSymlinkRoots(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateBYODWorkspace(root); err != nil {
		t.Fatal(err)
	}
	if err := validateBYODWorkspace(root + string(os.PathSeparator) + "."); !errors.Is(err, ErrProductionInvalid) {
		t.Fatalf("non-canonical error=%v", err)
	}
	link := filepath.Join(t.TempDir(), "workspace")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if err := validateBYODWorkspace(link); !errors.Is(err, ErrProductionInvalid) {
		t.Fatalf("symlink error=%v", err)
	}
	if err := validateBYODWorkspace("relative"); !errors.Is(err, ErrProductionInvalid) {
		t.Fatalf("relative error=%v", err)
	}
}

func TestRetryHostedControlWaitsForTransientFailure(t *testing.T) {
	attempts := 0
	started := time.Now()
	result, err := retryHostedControl(context.Background(), func(context.Context) (string, error) {
		attempts++
		if attempts == 1 {
			return "", errors.New("control plane is not ready")
		}
		return "ready", nil
	})
	if err != nil || result != "ready" || attempts != 2 || time.Since(started) < time.Second {
		t.Fatalf("result=%q err=%v attempts=%d elapsed=%s", result, err, attempts, time.Since(started))
	}
}

func TestRetryHostedControlStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	_, err := retryHostedControl(ctx, func(context.Context) (string, error) {
		attempts++
		cancel()
		return "", errors.New("unavailable")
	})
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
}

func TestProductionConnectorTransportDefaultsToAuto(t *testing.T) {
	for _, value := range []string{"", "  "} {
		if got := productionConnectorTransport(value); got != connector.Auto {
			t.Fatalf("transport(%q) = %q", value, got)
		}
	}
	if got := productionConnectorTransport("quic"); got != connector.QUIC {
		t.Fatalf("explicit transport = %q", got)
	}
}
