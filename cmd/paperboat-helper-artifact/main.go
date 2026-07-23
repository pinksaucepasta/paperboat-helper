// Command paperboat-helper-artifact creates development BYOD artifact metadata.
// It never generates, prints, or persists a signing private key.
package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const schema = "paperboat.helper-artifact/v1"

type manifest struct {
	Schema       string `json:"schema"`
	Version      string `json:"version"`
	Platform     string `json:"platform"`
	Architecture string `json:"architecture"`
	URL          string `json:"url"`
	ByteLength   int64  `json:"byte_length"`
	SHA256       string `json:"sha256"`
	Signature    string `json:"signature"`
}
type payload struct {
	Architecture string `json:"architecture"`
	ByteLength   int64  `json:"byte_length"`
	Platform     string `json:"platform"`
	Schema       string `json:"schema"`
	SHA256       string `json:"sha256"`
	URL          string `json:"url"`
	Version      string `json:"version"`
}

func main() {
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	artifactPath := flags.String("artifact", "", "artifact binary to hash")
	keyPath := flags.String("private-key", "", "base64 Ed25519 seed/private-key file")
	version := flags.String("version", "", "artifact version")
	platform := flags.String("platform", runtime.GOOS, "target platform")
	architecture := flags.String("architecture", runtime.GOARCH, "target architecture")
	publicURL := flags.String("url", "", "HTTPS URL where the artifact is served")
	manifestPath := flags.String("manifest-output", "", "output JSON array path")
	publicPath := flags.String("public-key-output", "", "output base64 public-key path")
	flags.Parse(os.Args[1:])
	if err := generate(*artifactPath, *keyPath, *version, *platform, *architecture, *publicURL, *manifestPath, *publicPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generate(artifactPath, keyPath, version, platform, architecture, publicURL, manifestPath, publicPath string) error {
	if strings.TrimSpace(artifactPath) == "" || strings.TrimSpace(keyPath) == "" || strings.TrimSpace(version) == "" || strings.TrimSpace(publicURL) == "" || strings.TrimSpace(manifestPath) == "" || strings.TrimSpace(publicPath) == "" || !filepath.IsAbs(artifactPath) || !filepath.IsAbs(keyPath) || !filepath.IsAbs(manifestPath) || !filepath.IsAbs(publicPath) {
		return errors.New("artifact, private-key, version, url, manifest-output, and public-key-output are required (absolute paths for files)")
	}
	parsedURL, err := url.Parse(publicURL)
	if err != nil || parsedURL.Scheme != "https" || parsedURL.User != nil || parsedURL.Hostname() == "" || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return errors.New("artifact URL must use HTTPS")
	}
	keyInfo, err := os.Stat(keyPath)
	if err != nil || keyInfo.Mode().Perm()&0o077 != 0 {
		return errors.New("private-key file must exist with owner-only permissions")
	}
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(string(keyBytes)))
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(strings.TrimSpace(string(keyBytes)))
	}
	if err != nil || (len(decoded) != ed25519.SeedSize && len(decoded) != ed25519.PrivateKeySize) {
		return errors.New("private-key must contain a base64 Ed25519 seed or private key")
	}
	private := ed25519.PrivateKey(decoded)
	if len(decoded) == ed25519.SeedSize {
		private = ed25519.NewKeyFromSeed(decoded)
	}
	artifactInfo, err := os.Stat(artifactPath)
	if err != nil || artifactInfo.Size() < 1 || artifactInfo.Size() > 256<<20 {
		return errors.New("artifact must be a readable non-empty file")
	}
	artifact, err := os.ReadFile(artifactPath)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(artifact)
	item := manifest{Schema: schema, Version: version, Platform: platform, Architecture: architecture, URL: publicURL, ByteLength: int64(len(artifact)), SHA256: hex.EncodeToString(digest[:])}
	encoded, err := json.Marshal(payload{Architecture: item.Architecture, ByteLength: item.ByteLength, Platform: item.Platform, Schema: item.Schema, SHA256: item.SHA256, URL: item.URL, Version: item.Version})
	if err != nil {
		return err
	}
	item.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, encoded))
	manifestJSON, err := json.MarshalIndent([]manifest{item}, "", "  ")
	if err != nil {
		return err
	}
	if err := writeOwnerOnly(manifestPath, append(manifestJSON, '\n')); err != nil {
		return err
	}
	publicJSON := []byte(base64.RawURLEncoding.EncodeToString(private.Public().(ed25519.PublicKey)) + "\n")
	return writeOwnerOnly(publicPath, publicJSON)
}

func writeOwnerOnly(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	_, err = file.Write(data)
	return err
}
