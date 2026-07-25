//go:build darwin || linux

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	got, err := resolveUserShell(shell, func(string) string { return "/bin/false" })
	if err != nil || got != shell {
		t.Fatalf("override shell=%q err=%v", got, err)
	}
	got, err = resolveUserShell("", func(name string) string {
		if name == "SHELL" {
			return shell
		}
		return ""
	})
	if err != nil || got != shell {
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
	manifest, publicKey := signedBootstrapArtifact(t, server.URL+"/pbh", expected)
	material := bootstrap.Material{Artifact: &manifest, ArtifactPublicKey: publicKey, EnrollmentCredential: "credential-that-must-not-be-consumed", ControlURL: "https://control.example.test"}
	root := t.TempDir()
	client := &recordingEnrollmentClient{}
	if _, err := prepareInstallation(context.Background(), &material, root, server.Client(), client); err != bootstrap.ErrArtifactMismatch {
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
	manifest, publicKey := signedBootstrapArtifact(t, artifactServer.URL+"/pbh", body)
	material := bootstrap.Material{UserMachineID: "um_reuse", UserMachineEnrollmentID: "ume_reuse", EnvironmentID: "env_reuse", HelperID: "helper_reuse", ReuseIdentity: true, Artifact: &manifest, ArtifactPublicKey: publicKey}
	client := &recordingEnrollmentClient{}
	path, err := prepareInstallation(context.Background(), &material, stateRoot, artifactServer.Client(), client)
	if err != nil {
		t.Fatal(err)
	}
	if client.calls != 0 || !strings.HasPrefix(path, filepath.Join(stateRoot, "artifacts")) {
		t.Fatalf("enrollment calls=%d path=%q", client.calls, path)
	}
	material.HelperID = "helper_other"
	if _, err := prepareInstallation(context.Background(), &material, stateRoot, artifactServer.Client(), client); err != bootstrap.ErrInvalid {
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

func signedBootstrapArtifact(t *testing.T, artifactURL string, body []byte) (bootstrap.ArtifactManifest, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	manifest := bootstrap.ArtifactManifest{Schema: bootstrap.ArtifactSchemaV1, Version: "test", Platform: runtime.GOOS, Architecture: runtime.GOARCH, URL: artifactURL, ByteLength: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
	payload, err := json.Marshal(struct {
		Architecture string `json:"architecture"`
		ByteLength   int64  `json:"byte_length"`
		Platform     string `json:"platform"`
		Schema       string `json:"schema"`
		SHA256       string `json:"sha256"`
		URL          string `json:"url"`
		Version      string `json:"version"`
	}{manifest.Architecture, manifest.ByteLength, manifest.Platform, manifest.Schema, manifest.SHA256, manifest.URL, manifest.Version})
	if err != nil {
		t.Fatal(err)
	}
	manifest.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return manifest, base64.RawURLEncoding.EncodeToString(publicKey)
}
