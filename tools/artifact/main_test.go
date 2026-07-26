package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

func TestGenerateProducesManifestAcceptedByBootstrap(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "paperboat-helper-linux-amd64")
	if err := os.WriteFile(artifactPath, executableHeader(runtime.GOOS, runtime.GOARCH), 0o700); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(root, "development-signing-key")
	if err := os.WriteFile(keyPath, []byte(base64.RawURLEncoding.EncodeToString(private.Seed())), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath, publicPath := filepath.Join(root, "artifacts.json"), filepath.Join(root, "public-key")
	if err := generate(artifactPath, keyPath, "0.0.0-development", runtime.GOOS, runtime.GOARCH, "worker", "https://downloads.example.test/paperboat-helper", manifestPath, publicPath); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifests []bootstrap.ArtifactManifest
	if json.Unmarshal(encoded, &manifests) != nil || len(manifests) != 1 {
		t.Fatalf("manifest=%s", encoded)
	}
	encodedPublic, err := os.ReadFile(publicPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(encodedPublic) != base64.RawURLEncoding.EncodeToString(public)+"\n" {
		t.Fatalf("public=%q", encodedPublic)
	}
	if err := bootstrap.VerifyArtifactManifest(manifests[0], string(encodedPublic[:len(encodedPublic)-1])); err != nil {
		t.Fatal(err)
	}
}

func executableHeader(platform, architecture string) []byte {
	header := make([]byte, 32)
	if platform == "linux" {
		copy(header, "\x7fELF")
		header[4], header[5] = 2, 1
		machine := uint16(62)
		if architecture == "arm64" {
			machine = 183
		}
		binary.LittleEndian.PutUint16(header[18:20], machine)
		return header
	}
	binary.LittleEndian.PutUint32(header[:4], 0xfeedfacf)
	cpu := uint32(0x01000007)
	if architecture == "arm64" {
		cpu = 0x0100000c
	}
	binary.LittleEndian.PutUint32(header[4:8], cpu)
	return header
}

func TestGenerateRejectsMislabeledExecutableTarget(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "artifact")
	if err := os.WriteFile(artifactPath, executableHeader("darwin", "arm64"), 0o700); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(root, "key")
	if err := os.WriteFile(keyPath, []byte(base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SeedSize))), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := generate(artifactPath, keyPath, "dev", "linux", "amd64", "worker", "https://downloads.example.test/artifact", filepath.Join(root, "manifest"), filepath.Join(root, "public")); err == nil {
		t.Fatal("mislabeled executable target accepted")
	}
}

func TestGenerateRejectsWeakPrivateKeyPermissions(t *testing.T) {
	root := t.TempDir()
	artifactPath, keyPath := filepath.Join(root, "artifact"), filepath.Join(root, "key")
	if err := os.WriteFile(artifactPath, []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, []byte(base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SeedSize))), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := generate(artifactPath, keyPath, "dev", "linux", "amd64", "worker", "https://downloads.example.test/artifact", filepath.Join(root, "manifest"), filepath.Join(root, "public")); err == nil {
		t.Fatal("weak private-key permissions accepted")
	}
}
