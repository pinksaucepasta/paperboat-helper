//go:build darwin || linux

package runtime

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifiedCachedChezmoiRejectsTampering(t *testing.T) {
	archive := testChezmoiArchive(t, tar.TypeReg, []byte("binary"))
	sum := sha256.Sum256(archive)
	root := t.TempDir()
	archivePath := filepath.Join(root, "chezmoi.tar.gz")
	binaryPath := filepath.Join(root, "chezmoi")
	if err := os.WriteFile(archivePath, archive, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binaryPath, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	digest := hex.EncodeToString(sum[:])
	if !verifiedCachedChezmoi(binaryPath, archivePath, digest) {
		t.Fatal("valid pinned cache rejected")
	}
	if err := os.WriteFile(binaryPath, []byte("tampered"), 0o700); err != nil {
		t.Fatal(err)
	}
	if verifiedCachedChezmoi(binaryPath, archivePath, digest) {
		t.Fatal("tampered executable accepted")
	}
}

func TestExtractChezmoiRejectsLinks(t *testing.T) {
	if _, err := extractChezmoi(testChezmoiArchive(t, tar.TypeSymlink, nil)); err == nil {
		t.Fatal("symlink executable accepted")
	}
}

func testChezmoiArchive(t *testing.T, kind byte, body []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	compressed := gzip.NewWriter(&output)
	archive := tar.NewWriter(compressed)
	if err := archive.WriteHeader(&tar.Header{Name: "chezmoi", Typeflag: kind, Size: int64(len(body)), Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if len(body) > 0 {
		if _, err := archive.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
