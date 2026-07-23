package bootstrap

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const ArtifactSchemaV1 = "paperboat.helper-artifact/v1"

var (
	ErrArtifactManifest  = errors.New("helper artifact manifest is invalid")
	ErrArtifactSignature = errors.New("helper artifact signature is invalid")
	ErrArtifactMismatch  = errors.New("helper artifact does not match its manifest")
)

type ArtifactManifest struct {
	Schema       string `json:"schema"`
	Version      string `json:"version"`
	Platform     string `json:"platform"`
	Architecture string `json:"architecture"`
	URL          string `json:"url"`
	ByteLength   int64  `json:"byte_length"`
	SHA256       string `json:"sha256"`
	Signature    string `json:"signature"`
}

// artifactSignaturePayload is declared in RFC 8785 lexicographic key order.
// These fields contain no floating-point values, so encoding/json produces the
// canonical representation required by the frozen helper-artifact contract.
type artifactSignaturePayload struct {
	Architecture string `json:"architecture"`
	ByteLength   int64  `json:"byte_length"`
	Platform     string `json:"platform"`
	Schema       string `json:"schema"`
	SHA256       string `json:"sha256"`
	URL          string `json:"url"`
	Version      string `json:"version"`
}

func (m ArtifactManifest) signaturePayload() ([]byte, error) {
	return json.Marshal(artifactSignaturePayload{
		Architecture: m.Architecture, ByteLength: m.ByteLength, Platform: m.Platform,
		Schema: m.Schema, SHA256: m.SHA256, URL: m.URL, Version: m.Version,
	})
}

func VerifyArtifactManifest(manifest ArtifactManifest, encodedPublicKey string) error {
	parsed, err := url.Parse(manifest.URL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" || manifest.Schema != ArtifactSchemaV1 || manifest.Version == "" || manifest.Platform != runtime.GOOS || manifest.Architecture != runtime.GOARCH || manifest.ByteLength < 1 || manifest.ByteLength > 256<<20 || len(manifest.SHA256) != sha256.Size*2 {
		return ErrArtifactManifest
	}
	if _, err := hex.DecodeString(manifest.SHA256); err != nil {
		return ErrArtifactManifest
	}
	publicKey, err := decodeBase64(encodedPublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return ErrArtifactSignature
	}
	signature, err := decodeBase64(manifest.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return ErrArtifactSignature
	}
	payload, err := manifest.signaturePayload()
	if err != nil || !ed25519.Verify(ed25519.PublicKey(publicKey), payload, signature) {
		return ErrArtifactSignature
	}
	return nil
}

func FetchVerifiedArtifact(ctx context.Context, manifest ArtifactManifest, encodedPublicKey, directory string, httpClient *http.Client) (string, error) {
	if err := VerifyArtifactManifest(manifest, encodedPublicKey); err != nil {
		return "", err
	}
	if !filepath.IsAbs(directory) {
		return "", ErrArtifactManifest
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return "", ErrArtifactManifest
	}
	if httpClient == nil {
		httpClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return ErrArtifactManifest }}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, manifest.URL, nil)
	if err != nil {
		return "", err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ContentLength > manifest.ByteLength {
		return "", ErrArtifactMismatch
	}
	file, err := os.CreateTemp(directory, ".paperboat-helper-artifact-*")
	if err != nil {
		return "", err
	}
	path := file.Name()
	success := false
	defer func() {
		_ = file.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	if err := file.Chmod(0o700); err != nil {
		return "", err
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, manifest.ByteLength+1))
	if err != nil {
		return "", err
	}
	if written != manifest.ByteLength || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), manifest.SHA256) {
		return "", ErrArtifactMismatch
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	success = true
	return path, nil
}

func decodeBase64(value string) ([]byte, error) {
	if decoded, err := base64.RawURLEncoding.DecodeString(value); err == nil {
		return decoded, nil
	}
	return base64.StdEncoding.DecodeString(value)
}
