//go:build darwin || linux

package runtime

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
)

func validPhase2HarnessDocument(t *testing.T) Phase2HarnessConfig {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	return Phase2HarnessConfig{
		Profile: helperconfig.BYOD, StateRoot: filepath.Join(root, "state"), WorkspaceRoot: root,
		ListenAddress: "127.0.0.1:0", ShellPath: "/bin/sh", ShellArgs: []string{"-l"},
		ShellEnvironment: []string{"PATH=/usr/bin:/bin"}, OriginPatterns: []string{"control.test"},
		Issuer: "https://control.test", EnvironmentID: "env_test", HelperID: "hlp_test",
		PublicKeys: map[string]string{"key-1": base64.RawURLEncoding.EncodeToString(public)},
	}
}

func writePhase2HarnessConfig(t *testing.T, encoded []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "phase2-harness.json")
	if err := os.WriteFile(path, encoded, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadPhase2HarnessConfigAcceptsStrictPrivateConfiguration(t *testing.T) {
	document := validPhase2HarnessDocument(t)
	encoded, _ := json.Marshal(document)
	loaded, err := LoadPhase2HarnessConfig(writePhase2HarnessConfig(t, encoded, 0o600))
	if err != nil || loaded.EnvironmentID != document.EnvironmentID || loaded.ListenAddress != "127.0.0.1:0" || len(loaded.PublicKeys) != 1 {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
}

func TestLoadPhase2HarnessConfigRejectsAmbiguousOrUnsafeTrustFiles(t *testing.T) {
	document := validPhase2HarnessDocument(t)
	encoded, _ := json.Marshal(document)
	duplicate := append([]byte(`{"profile":"byod","profile":"hosted",`), encoded[1:]...)
	unknown := append(bytes.TrimSuffix(encoded, []byte("}")), []byte(`,"unknown":true}`)...)
	unsafeBind := document
	unsafeBind.ListenAddress = "0.0.0.0:8080"
	unsafeEncoded, _ := json.Marshal(unsafeBind)
	invalidKey := document
	invalidKey.PublicKeys = map[string]string{"key-1": "invalid"}
	invalidKeyEncoded, _ := json.Marshal(invalidKey)
	for name, value := range map[string][]byte{"duplicate": duplicate, "unknown": unknown, "unsafe_bind": unsafeEncoded, "invalid_key": invalidKeyEncoded} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadPhase2HarnessConfig(writePhase2HarnessConfig(t, value, 0o600)); !errors.Is(err, ErrHelperInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	worldWritable := writePhase2HarnessConfig(t, encoded, 0o622)
	if _, err := LoadPhase2HarnessConfig(worldWritable); !errors.Is(err, ErrHelperInvalid) {
		t.Fatalf("world-writable err=%v", err)
	}
	hardlink := filepath.Join(t.TempDir(), "linked.json")
	if err := os.Link(writePhase2HarnessConfig(t, encoded, 0o600), hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPhase2HarnessConfig(hardlink); !errors.Is(err, ErrHelperInvalid) {
		t.Fatalf("hardlink err=%v", err)
	}
	symlink := filepath.Join(t.TempDir(), "symlink.json")
	if err := os.Symlink(writePhase2HarnessConfig(t, encoded, 0o600), symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPhase2HarnessConfig(symlink); !errors.Is(err, ErrHelperInvalid) {
		t.Fatalf("symlink err=%v", err)
	}
}
