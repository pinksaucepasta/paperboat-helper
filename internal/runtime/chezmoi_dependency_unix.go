//go:build darwin || linux

package runtime

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	goruntime "runtime"
)

const chezmoiVersion = "2.71.0"

var chezmoiDigests = map[string]string{
	"darwin/amd64": "12b78b365528597ad701f5117fa71f6c42b5b1e65d8075e19c48472ad81faf30",
	"darwin/arm64": "8b03d7be6b5d500a503c712ae6da7dd6817b6c3328223b4ae8be5a2fa3a",
	"linux/amd64":  "6ea2040ecc0e82d3dac604289e100b0157afefcd94ebb818e5f6e31655156d34",
	"linux/arm64":  "d8fb35f9d43237b4f6d022cad40e1094957b990cfaee5f3b131ded65422b0983",
}

func ensureChezmoi(ctx context.Context, configured, stateRoot string, client *http.Client) (string, error) {
	if executableRegularFile(configured) {
		return configured, nil
	}
	if configured != "/usr/local/bin/chezmoi" || !filepath.IsAbs(stateRoot) || client == nil {
		return "", ErrProductionInvalid
	}
	digest, ok := chezmoiDigests[goruntime.GOOS+"/"+goruntime.GOARCH]
	if !ok {
		return "", ErrProductionInvalid
	}
	directory := filepath.Join(stateRoot, "dependencies")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	destination := filepath.Join(directory, "chezmoi-"+chezmoiVersion)
	archivePath := destination + ".tar.gz"
	if verifiedCachedChezmoi(destination, archivePath, digest) {
		return destination, nil
	}
	url := "https://github.com/twpayne/chezmoi/releases/download/v" + chezmoiVersion +
		"/chezmoi_" + chezmoiVersion + "_" + goruntime.GOOS + "_" + goruntime.GOARCH + ".tar.gz"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ContentLength > 64<<20 {
		return "", ErrProductionInvalid
	}
	archive, err := io.ReadAll(io.LimitReader(response.Body, (64<<20)+1))
	if err != nil || len(archive) > 64<<20 {
		return "", errors.Join(ErrProductionInvalid, err)
	}
	sum := sha256.Sum256(archive)
	if hex.EncodeToString(sum[:]) != digest {
		return "", ErrProductionInvalid
	}
	binary, err := extractChezmoi(archive)
	if err != nil {
		return "", err
	}
	if err := writeDependencyAtomic(archivePath, archive, 0o600); err != nil {
		return "", err
	}
	if err := writeDependencyAtomic(destination, binary, 0o700); err != nil {
		return "", err
	}
	return destination, nil
}

func writeDependencyAtomic(destination string, value []byte, mode os.FileMode) error {
	directory := filepath.Dir(destination)
	temporary, err := os.CreateTemp(directory, ".chezmoi-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err = temporary.Chmod(mode); err == nil {
		_, err = temporary.Write(value)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(temporaryPath, destination)
	}
	return err
}

func extractChezmoi(archive []byte) ([]byte, error) {
	compressed, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, ErrProductionInvalid
	}
	defer compressed.Close()
	reader := tar.NewReader(compressed)
	var binary []byte
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, ErrProductionInvalid
		}
		if header.Name != "chezmoi" {
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size < 1 || header.Size > 64<<20 || binary != nil {
			return nil, ErrProductionInvalid
		}
		binary, err = io.ReadAll(io.LimitReader(reader, header.Size+1))
		if err != nil || int64(len(binary)) != header.Size {
			return nil, ErrProductionInvalid
		}
	}
	if len(binary) == 0 {
		return nil, ErrProductionInvalid
	}
	return binary, nil
}

func verifiedCachedChezmoi(destination, archivePath, digest string) bool {
	if !executableRegularFile(destination) {
		return false
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil || len(archive) > 64<<20 {
		return false
	}
	sum := sha256.Sum256(archive)
	if hex.EncodeToString(sum[:]) != digest {
		return false
	}
	expected, err := extractChezmoi(archive)
	if err != nil {
		return false
	}
	actual, err := os.ReadFile(destination)
	return err == nil && bytes.Equal(actual, expected)
}

func executableRegularFile(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 &&
		info.Mode().Perm()&0o100 != 0 && info.Mode().Perm()&0o022 == 0
}
