//go:build darwin || linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestFetchPinnedExecutableVerifiesAndReusesArtifact(t *testing.T) {
	content := []byte("verified Herdr")
	digest := sha256.Sum256(content)
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = writer.Write(content)
	}))
	defer server.Close()
	artifact := pinnedExecutable{URL: server.URL, Size: int64(len(content)), SHA256: hex.EncodeToString(digest[:]), FileName: "herdr-test"}
	directory := filepath.Join(t.TempDir(), "artifacts")
	first, err := fetchPinnedExecutable(context.Background(), artifact, directory, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	second, err := fetchPinnedExecutable(context.Background(), artifact, directory, server.Client())
	if err != nil || first != second || requests != 1 {
		t.Fatalf("first=%q second=%q requests=%d err=%v", first, second, requests, err)
	}
	if info, err := os.Stat(first); err != nil || info.Mode().Perm() != 0o500 {
		t.Fatalf("info=%v err=%v", info, err)
	}
}

func TestFetchPinnedExecutableRejectsMismatchAndCleansTemporary(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte("wrong")) }))
	defer server.Close()
	directory := filepath.Join(t.TempDir(), "artifacts")
	artifact := pinnedExecutable{URL: server.URL, Size: 5, SHA256: hex.EncodeToString(make([]byte, sha256.Size)), FileName: "herdr-test"}
	if _, err := fetchPinnedExecutable(context.Background(), artifact, directory, server.Client()); err == nil {
		t.Fatal("expected mismatch")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestHerdrReleaseCoversSupportedBootstrapPlatforms(t *testing.T) {
	for _, key := range []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64"} {
		if release := herdrReleases[key]; release.Size < 1 || len(release.SHA256) != sha256.Size*2 {
			t.Fatalf("release %s=%#v", key, release)
		}
	}
	if _, ok := herdrReleases[runtime.GOOS+"/"+runtime.GOARCH]; !ok {
		t.Fatalf("current platform %s/%s missing", runtime.GOOS, runtime.GOARCH)
	}
}
