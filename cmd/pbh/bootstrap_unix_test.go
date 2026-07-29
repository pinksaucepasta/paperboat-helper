//go:build darwin || linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
	"github.com/pinksaucepasta/paperboat-helper/internal/enrollment"
)

func TestConfirmRootBootstrapRequiresExplicitYes(t *testing.T) {
	for _, input := range []string{"", "y\n", "YES\n", "no\n"} {
		var output bytes.Buffer
		if err := confirmRootBootstrap(bufio.NewReader(strings.NewReader(input)), &output); err == nil {
			t.Fatalf("input %q was accepted", input)
		}
		if !strings.Contains(output.String(), "full control of this machine") {
			t.Fatalf("warning missing for input %q: %q", input, output.String())
		}
	}
	var output bytes.Buffer
	if err := confirmRootBootstrap(bufio.NewReader(strings.NewReader("yes\n")), &output); err != nil {
		t.Fatal(err)
	}
}

type recordingEnrollmentClient struct{ calls int }

func (c *recordingEnrollmentClient) Enroll(context.Context, enrollment.Config) (enrollment.RuntimeIdentity, error) {
	c.calls++
	return enrollment.RuntimeIdentity{}, nil
}

type failingInstaller struct {
	calls     int
	failUntil int
}

func (i *failingInstaller) Install(context.Context) error {
	i.calls++
	if i.calls <= i.failUntil {
		return os.ErrPermission
	}
	return nil
}

func TestInstallServiceRetriesBoundedly(t *testing.T) {
	installer := &failingInstaller{failUntil: 1}
	if err := installService(context.Background(), installer, 3, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if installer.calls != 2 {
		t.Fatalf("install calls = %d", installer.calls)
	}
	installer = &failingInstaller{failUntil: 3}
	if err := installService(context.Background(), installer, 3, time.Millisecond); err == nil || installer.calls != 3 {
		t.Fatalf("error=%v calls=%d", err, installer.calls)
	}
}

func TestResolveUserShellUsesOverrideAndEnvironment(t *testing.T) {
	shell := filepath.Join(t.TempDir(), "shell")
	if err := os.WriteFile(shell, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	canonicalShell, err := filepath.EvalSymlinks(shell)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolveUserShell(shell, func(string) string { return "/bin/false" })
	if err != nil || got != canonicalShell {
		t.Fatalf("override shell=%q err=%v", got, err)
	}
	got, err = resolveUserShell("", func(name string) string {
		if name == "SHELL" {
			return shell
		}
		return ""
	})
	if err != nil || got != canonicalShell {
		t.Fatalf("detected shell=%q err=%v", got, err)
	}
	if _, err := resolveUserShell("relative-shell", nil); err == nil {
		t.Fatal("relative shell override was accepted")
	}
}

func TestCanonicalUserHomeAndRemovedWorkspaceOverride(t *testing.T) {
	home, err := canonicalUserHome()
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if home != filepath.Clean(want) {
		t.Fatalf("home=%q want=%q", home, want)
	}
	if err := runBootstrap(context.Background(), []string{"--workspace", t.TempDir()}, strings.NewReader(""), io.Discard, io.Discard); err == nil {
		t.Fatal("removed workspace override was accepted")
	}
}

func TestPrepareInstallationVerifiesArtifactBeforeEnrollment(t *testing.T) {
	expected := []byte("expected helper")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tampered helper"))
	}))
	defer server.Close()
	manifest, hostManifest, publicKey := signedBootstrapArtifacts(t, server.URL+"/pbh", expected)
	material := bootstrap.Material{Artifact: &manifest, HostServiceArtifact: &hostManifest, ArtifactPublicKey: publicKey, EnrollmentCredential: "credential-that-must-not-be-consumed", ControlURL: "https://control.example.test"}
	root := t.TempDir()
	client := &recordingEnrollmentClient{}
	if _, _, err := prepareInstallation(context.Background(), &material, root, server.Client(), client); err != bootstrap.ErrArtifactMismatch {
		t.Fatalf("error = %v", err)
	}
	if client.calls != 0 || material.EnrollmentCredential == "" {
		t.Fatalf("enrollment calls=%d credential_cleared=%v", client.calls, material.EnrollmentCredential == "")
	}
	entries, err := os.ReadDir(filepath.Join(root, "artifacts"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("artifact entries=%v error=%v", entries, err)
	}
}

func TestPrepareInstallationReusesMatchingPersistedIdentity(t *testing.T) {
	stateRoot := enrolledStateRoot(t, "helper_reuse", "env_reuse")
	body := []byte("verified helper")
	artifactServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer artifactServer.Close()
	manifest, hostManifest, publicKey := signedBootstrapArtifacts(t, artifactServer.URL+"/pbh", body)
	material := bootstrap.Material{UserMachineID: "um_reuse", UserMachineEnrollmentID: "ume_reuse", EnvironmentID: "env_reuse", HelperID: "helper_reuse", ReuseIdentity: true, Artifact: &manifest, HostServiceArtifact: &hostManifest, ArtifactPublicKey: publicKey}
	client := &recordingEnrollmentClient{}
	path, hostPath, err := prepareInstallation(context.Background(), &material, stateRoot, artifactServer.Client(), client)
	if err != nil {
		t.Fatal(err)
	}
	if client.calls != 0 || !strings.HasPrefix(path, filepath.Join(stateRoot, "artifacts")) || !strings.HasPrefix(hostPath, filepath.Join(stateRoot, "artifacts")) {
		t.Fatalf("enrollment calls=%d paths=%q,%q", client.calls, path, hostPath)
	}
	material.HelperID = "helper_other"
	if _, _, err := prepareInstallation(context.Background(), &material, stateRoot, artifactServer.Client(), client); err != bootstrap.ErrInvalid {
		t.Fatalf("mismatched identity error = %v", err)
	}
}

func TestReportInstallationFailureUsesSignedPersistedIdentity(t *testing.T) {
	stateRoot := enrolledStateRoot(t, "helper_failure", "env_failure")
	var received bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/user-machine-installation-failures" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer identity-") || r.Header.Get("X-Paperboat-Helper-Proof") == "" {
			t.Errorf("request path=%q auth=%q proof=%q", r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Paperboat-Helper-Proof"))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		received = true
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	material := bootstrap.Material{UserMachineEnrollmentID: "ume_failure", EnvironmentID: "env_failure", HelperID: "helper_failure", ControlURL: server.URL}
	if err := reportInstallationFailureWithClient(context.Background(), material, stateRoot, "service_install", server.Client()); err != nil {
		t.Fatal(err)
	}
	if !received {
		t.Fatal("failure report was not received")
	}
}

func TestReportArtifactFailureUsesOneTimeEnrollmentCredential(t *testing.T) {
	var received bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/user-machine-installation-failures" || r.Header.Get("Authorization") != "Bearer one-time-enrollment" || r.Header.Get("X-Paperboat-Helper-Proof") != "" {
			t.Errorf("request path=%q auth=%q proof=%q", r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Paperboat-Helper-Proof"))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		received = true
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	material := bootstrap.Material{UserMachineEnrollmentID: "ume_artifact", EnvironmentID: "env_artifact", HelperID: "helper_artifact", EnrollmentID: "henr_artifact", EnrollmentCredential: "one-time-enrollment", ControlURL: server.URL}
	if err := reportInstallationFailureWithEnrollmentCredentialClient(context.Background(), material, "artifact_verification", server.Client()); err != nil {
		t.Fatal(err)
	}
	if !received {
		t.Fatal("artifact failure report was not received")
	}
}

func TestRemoveNewEnrollmentCredentialsPreservesDurableState(t *testing.T) {
	stateRoot := enrolledStateRoot(t, "helper_cleanup", "env_cleanup")
	durable := filepath.Join(stateRoot, "sessions.db")
	if err := os.WriteFile(durable, []byte("durable"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeNewEnrollmentCredentials(stateRoot); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"runtime-identity.json", "helper-identity.json"} {
		if _, err := os.Lstat(filepath.Join(stateRoot, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("credential %s remains: %v", name, err)
		}
	}
	if body, err := os.ReadFile(durable); err != nil || string(body) != "durable" {
		t.Fatalf("durable state changed: body=%q err=%v", body, err)
	}
}

func TestArtifactHTTPClientAllowsOnlyGitHubReleaseAssetRedirect(t *testing.T) {
	check := artifactHTTPClient().CheckRedirect
	origin := httptest.NewRequest(http.MethodGet, "https://github.com/pinksaucepasta/paperboat-helper/releases/download/version/pbh", nil)
	allowed := httptest.NewRequest(http.MethodGet, "https://release-assets.githubusercontent.com/github-production-release-asset/file?signature=value", nil)
	if err := check(allowed, []*http.Request{origin}); err != nil {
		t.Fatalf("valid GitHub release redirect rejected: %v", err)
	}

	for name, target := range map[string]string{
		"insecure":    "http://release-assets.githubusercontent.com/file",
		"credentials": "https://user@release-assets.githubusercontent.com/file",
		"other host":  "https://example.com/file",
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, target, nil)
			if err := check(request, []*http.Request{origin}); !errors.Is(err, bootstrap.ErrArtifactManifest) {
				t.Fatalf("redirect error = %v", err)
			}
		})
	}
	if err := check(allowed, []*http.Request{origin, allowed}); !errors.Is(err, bootstrap.ErrArtifactManifest) {
		t.Fatalf("second redirect error = %v", err)
	}
}

func enrolledStateRoot(t *testing.T, helperID, environmentID string) string {
	t.Helper()
	root := t.TempDir()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": enrollment.RuntimeIdentity{HelperID: helperID, EnvironmentID: environmentID, Credential: "identity-" + strings.Repeat("x", 40), ExpiresAt: time.Now().UTC().Add(time.Hour)}})
	}))
	defer server.Close()
	client, err := enrollment.NewClient(server.Client().Transport, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enroll(context.Background(), enrollment.Config{ControlURL: server.URL, StateRoot: root, EnrollmentCredential: strings.Repeat("e", 40)}); err != nil {
		t.Fatal(err)
	}
	return root
}

func signedBootstrapArtifacts(t *testing.T, artifactURL string, body []byte) (bootstrap.ArtifactManifest, bootstrap.ArtifactManifest, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(kind string) bootstrap.ArtifactManifest {
		digest := sha256.Sum256(body)
		manifest := bootstrap.ArtifactManifest{Schema: bootstrap.ArtifactSchemaV2, Kind: kind, Version: "test", Platform: runtime.GOOS, Architecture: runtime.GOARCH, URL: artifactURL, ByteLength: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
		payload, err := json.Marshal(struct {
			Architecture string `json:"architecture"`
			ByteLength   int64  `json:"byte_length"`
			Kind         string `json:"kind"`
			Platform     string `json:"platform"`
			Schema       string `json:"schema"`
			SHA256       string `json:"sha256"`
			URL          string `json:"url"`
			Version      string `json:"version"`
		}{manifest.Architecture, manifest.ByteLength, manifest.Kind, manifest.Platform, manifest.Schema, manifest.SHA256, manifest.URL, manifest.Version})
		if err != nil {
			t.Fatal(err)
		}
		manifest.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
		return manifest
	}
	return sign(bootstrap.ArtifactKindWorker), sign(bootstrap.ArtifactKindHostService), base64.RawURLEncoding.EncodeToString(publicKey)
}

func TestBootstrapHealthMatchesSignedArtifactVersion(t *testing.T) {
	response := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
	}
	if !bootstrapHealthMatches(response(http.StatusOK, `{"live":true,"version":"2026.07.26","capabilities":{},"checked_at":"2026-07-26T00:00:00Z"}`), "2026.07.26") {
		t.Fatal("matching signed artifact version was not ready")
	}
	for name, candidate := range map[string]*http.Response{
		"legacy version": response(http.StatusOK, `{"live":true,"version":"legacy","capabilities":{},"checked_at":"2026-07-26T00:00:00Z"}`),
		"not live":       response(http.StatusOK, `{"live":false,"version":"2026.07.26","capabilities":{},"checked_at":"2026-07-26T00:00:00Z"}`),
		"trailing data":  response(http.StatusOK, `{"live":true,"version":"2026.07.26","capabilities":{},"checked_at":"2026-07-26T00:00:00Z"} {}`),
		"wrong status":   response(http.StatusServiceUnavailable, `{}`),
	} {
		t.Run(name, func(t *testing.T) {
			if bootstrapHealthMatches(candidate, "2026.07.26") {
				t.Fatal("invalid health response accepted")
			}
		})
	}
}

func TestWorkerGenerationRequiresStrictDurableState(t *testing.T) {
	root := t.TempDir()
	runtimeRoot := filepath.Join(root, "runtime")
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(runtimeRoot, "worker-boot.json")
	valid := `{"schema":"paperboat.worker-boot/v1","os_boot_id":"boot-1","generation":4,"started_at":"2026-07-26T00:00:00Z"}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	if generation := workerGeneration(root); generation != 4 {
		t.Fatalf("generation = %d", generation)
	}
	if err := os.WriteFile(path, []byte(strings.TrimSuffix(valid, "}")+`,"unexpected":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if generation := workerGeneration(root); generation != 0 {
		t.Fatalf("hostile state generation = %d", generation)
	}
}

func TestServerHeartbeatReadinessBindsVersionAndNewGeneration(t *testing.T) {
	root := t.TempDir()
	runtimeRoot := filepath.Join(root, "runtime")
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(generation uint64, version string) {
		body, err := json.Marshal(map[string]any{"schema": "paperboat.server-heartbeat/v1", "worker_generation": generation, "reporter_version": version, "accepted_at": "2026-07-26T00:00:00Z"})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(runtimeRoot, "server-heartbeat.json"), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(5, "2026.07.25.4")
	if !serverHeartbeatReady(root, "2026.07.25.4", 4) {
		t.Fatal("new generation heartbeat was not ready")
	}
	if serverHeartbeatReady(root, "2026.07.25.5", 4) || serverHeartbeatReady(root, "2026.07.25.4", 5) {
		t.Fatal("stale or wrong-version heartbeat was accepted")
	}
}

func TestInstallHelperCommandTargetsCanonicalWorkerAndRestoresPreviousLink(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	commandPath := filepath.Join(directory, "pbh")
	previous := "/tmp/previous-paperboat-helper"
	if err := os.Symlink(previous, commandPath); err != nil {
		t.Fatal(err)
	}
	installation, err := installHelperCommand(directory, systemWorkerExecutable())
	if err != nil {
		t.Fatal(err)
	}
	if target, err := os.Readlink(commandPath); err != nil || target != systemWorkerExecutable() {
		t.Fatalf("target=%q err=%v", target, err)
	}
	if err := installation.Rollback(); err != nil {
		t.Fatal(err)
	}
	if target, err := os.Readlink(commandPath); err != nil || target != previous {
		t.Fatalf("restored target=%q err=%v", target, err)
	}
}

func TestInstallHelperCommandReplacesAndRestoresRegularExecutable(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	commandPath := filepath.Join(directory, "pbh")
	if err := os.WriteFile(commandPath, []byte("user-owned"), 0o700); err != nil {
		t.Fatal(err)
	}
	installation, err := installHelperCommand(directory, systemWorkerExecutable())
	if err != nil {
		t.Fatal(err)
	}
	if target, err := os.Readlink(commandPath); err != nil || target != systemWorkerExecutable() {
		t.Fatalf("target=%q err=%v", target, err)
	}
	if err := installation.Rollback(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(commandPath)
	if err != nil || string(contents) != "user-owned" {
		t.Fatalf("contents=%q err=%v", contents, err)
	}
}

func TestInstallHelperCommandCommitsRegularExecutableReplacement(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	commandPath := filepath.Join(directory, "pbh")
	if err := os.WriteFile(commandPath, []byte("old executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	installation, err := installHelperCommand(directory, systemWorkerExecutable())
	if err != nil {
		t.Fatal(err)
	}
	if err := installation.Commit(); err != nil {
		t.Fatal(err)
	}
	if target, err := os.Readlink(commandPath); err != nil || target != systemWorkerExecutable() {
		t.Fatalf("target=%q err=%v", target, err)
	}
	matches, err := filepath.Glob(filepath.Join(directory, ".pbh-backup-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("backups=%v err=%v", matches, err)
	}
}

func TestRemoveSystemHelperCommandOnlyRemovesCanonicalSymlink(t *testing.T) {
	for name, target := range map[string]struct {
		target  string
		removed bool
	}{
		"canonical": {target: systemWorkerExecutable(), removed: true},
		"unrelated": {target: "/tmp/user-helper", removed: false},
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			directory := filepath.Join(home, ".local", "bin")
			if err := os.MkdirAll(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			commandPath := filepath.Join(directory, "pbh")
			if err := os.Symlink(target.target, commandPath); err != nil {
				t.Fatal(err)
			}
			if err := removeSystemHelperCommand(); err != nil {
				t.Fatal(err)
			}
			_, err := os.Lstat(commandPath)
			if target.removed && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("canonical command remains: %v", err)
			}
			if !target.removed && err != nil {
				t.Fatalf("unrelated command removed: %v", err)
			}
		})
	}
}
