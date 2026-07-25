//go:build darwin || linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const herdrVersion = "0.7.4"

type pinnedExecutable struct {
	URL      string
	Size     int64
	SHA256   string
	FileName string
}

var herdrReleases = map[string]pinnedExecutable{
	"linux/amd64":  {URL: "https://github.com/ogulcancelik/herdr/releases/download/v0.7.4/herdr-linux-x86_64", Size: 19073408, SHA256: "bc0fc02d4ba500f9cac2353a43e67fe036785ecca6eb55378e050fac3c103059", FileName: "herdr-0.7.4-linux-amd64"},
	"linux/arm64":  {URL: "https://github.com/ogulcancelik/herdr/releases/download/v0.7.4/herdr-linux-aarch64", Size: 17438024, SHA256: "544e0002de42806d1ab64ccdef3a7e7414f24717b0b6b022bc9e57d2eefd26a2", FileName: "herdr-0.7.4-linux-arm64"},
	"darwin/amd64": {URL: "https://github.com/ogulcancelik/herdr/releases/download/v0.7.4/herdr-macos-x86_64", Size: 17081552, SHA256: "ddf430133352e1712413d5d865b34a485546f4658893fc89986257d65a7585a8", FileName: "herdr-0.7.4-darwin-amd64"},
	"darwin/arm64": {URL: "https://github.com/ogulcancelik/herdr/releases/download/v0.7.4/herdr-macos-aarch64", Size: 15866512, SHA256: "24992e1625dbdcb18354a59e299e4b263c312400b31396cdc07cd46ed57f24a7", FileName: "herdr-0.7.4-darwin-arm64"},
}

func installHerdr(ctx context.Context, stateRoot, platform, architecture string, client *http.Client) (string, error) {
	release, ok := herdrReleases[platform+"/"+architecture]
	if !ok {
		return "", fmt.Errorf("unsupported Herdr platform %s/%s", platform, architecture)
	}
	return fetchPinnedExecutable(ctx, release, filepath.Join(stateRoot, "artifacts"), client)
}

func fetchPinnedExecutable(ctx context.Context, artifact pinnedExecutable, directory string, client *http.Client) (string, error) {
	parsed, err := url.Parse(artifact.URL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" || artifact.Size < 1 || artifact.Size > 256<<20 || len(artifact.SHA256) != sha256.Size*2 || filepath.Base(artifact.FileName) != artifact.FileName || !filepath.IsAbs(directory) || client == nil {
		return "", errors.New("pinned executable metadata is invalid")
	}
	if _, err := hex.DecodeString(artifact.SHA256); err != nil {
		return "", errors.New("pinned executable metadata is invalid")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return "", errors.New("pinned executable directory is invalid")
	}
	destination := filepath.Join(directory, artifact.FileName)
	if validPinnedExecutable(destination, artifact) {
		return destination, nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, artifact.URL, nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ContentLength > artifact.Size {
		return "", errors.New("pinned executable download does not match metadata")
	}
	file, err := os.CreateTemp(directory, ".herdr-*")
	if err != nil {
		return "", err
	}
	temporary := file.Name()
	success := false
	defer func() {
		_ = file.Close()
		if !success {
			_ = os.Remove(temporary)
		}
	}()
	if err := file.Chmod(0o500); err != nil {
		return "", err
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, artifact.Size+1))
	if err != nil || written != artifact.Size || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), artifact.SHA256) {
		return "", errors.New("pinned executable download does not match metadata")
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(temporary, destination); err != nil {
		return "", err
	}
	success = true
	return destination, nil
}

func validPinnedExecutable(path string, artifact pinnedExecutable) bool {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != artifact.Size || info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o100 == 0 {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return false
	}
	return strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), artifact.SHA256)
}

func herdrHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if len(via) >= 5 || request.URL.Scheme != "https" || request.URL.User != nil {
			return errors.New("Herdr download redirect rejected")
		}
		host := strings.ToLower(request.URL.Hostname())
		if host != "github.com" && !strings.HasSuffix(host, ".githubusercontent.com") {
			return errors.New("Herdr download redirect rejected")
		}
		return nil
	}}
}
