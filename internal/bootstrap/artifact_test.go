package bootstrap

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
)

func signedArtifact(t *testing.T, serverURL string, artifact []byte) (ArtifactManifest, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifact)
	manifest := ArtifactManifest{Schema: ArtifactSchemaV1, Kind: ArtifactKindWorker, Version: "0.0.0-development", Platform: runtime.GOOS, Architecture: runtime.GOARCH, URL: serverURL + "/paperboat-helper", ByteLength: int64(len(artifact)), SHA256: hex.EncodeToString(digest[:])}
	payload, err := manifest.signaturePayload()
	if err != nil {
		t.Fatal(err)
	}
	manifest.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return manifest, base64.RawURLEncoding.EncodeToString(publicKey)
}

func signedArtifactPair(t *testing.T, serverURL string) (ArtifactManifest, ArtifactManifest, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(kind, name, body string) ArtifactManifest {
		digest := sha256.Sum256([]byte(body))
		manifest := ArtifactManifest{Schema: ArtifactSchemaV1, Kind: kind, Version: "0.0.0-development", Platform: runtime.GOOS, Architecture: runtime.GOARCH, URL: serverURL + "/" + name, ByteLength: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
		payload, err := manifest.signaturePayload()
		if err != nil {
			t.Fatal(err)
		}
		manifest.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
		return manifest
	}
	return sign(ArtifactKindWorker, "pbh", "helper"), sign(ArtifactKindHostService, "paperboat-host-service", "host"), base64.RawURLEncoding.EncodeToString(publicKey)
}

func TestFetchVerifiedArtifact(t *testing.T) {
	artifact := []byte("verified helper binary")
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", "22")
		_, _ = writer.Write(artifact)
	}))
	defer server.Close()
	manifest, publicKey := signedArtifact(t, server.URL, artifact)
	path, err := FetchVerifiedArtifact(context.Background(), manifest, publicKey, t.TempDir(), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(artifact) {
		t.Fatalf("artifact=%q err=%v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("mode=%v err=%v", info.Mode(), err)
	}
}

func TestFetchVerifiedArtifactRejectsMismatchAndCleansUp(t *testing.T) {
	artifact := []byte("expected")
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("tampered"))
	}))
	defer server.Close()
	manifest, publicKey := signedArtifact(t, server.URL, artifact)
	directory := t.TempDir()
	if _, err := FetchVerifiedArtifact(context.Background(), manifest, publicKey, directory, server.Client()); err != ErrArtifactMismatch {
		t.Fatalf("err=%v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestVerifyArtifactManifestRejectsWrongSignatureAndPlatform(t *testing.T) {
	manifest, publicKey := signedArtifact(t, "https://updates.example.test", []byte("helper"))
	manifest.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	if err := VerifyArtifactManifest(manifest, publicKey); err != ErrArtifactSignature {
		t.Fatalf("signature err=%v", err)
	}
	manifest, publicKey = signedArtifact(t, "https://updates.example.test", []byte("helper"))
	manifest.Platform = "unsupported"
	if err := VerifyArtifactManifest(manifest, publicKey); err != ErrArtifactManifest {
		t.Fatalf("platform err=%v", err)
	}
}
