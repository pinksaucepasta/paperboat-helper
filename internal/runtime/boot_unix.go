//go:build darwin || linux

package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"time"
)

const workerBootSchemaV1 = "paperboat.worker-boot/v1"

type workerBootState struct {
	Schema     string    `json:"schema"`
	OSBootID   string    `json:"os_boot_id"`
	Generation uint64    `json:"generation"`
	StartedAt  time.Time `json:"started_at"`
}

func recordWorkerBoot(stateRoot string) (workerBootState, string, error) {
	bootID, err := operatingSystemBootID()
	if err != nil {
		return workerBootState{}, "", err
	}
	path := filepath.Join(stateRoot, "runtime", "worker-boot.json")
	previous, loadErr := loadWorkerBoot(path)
	reason := "helper_restart"
	if loadErr == nil && previous.OSBootID != bootID {
		reason = "machine_reboot"
	} else if loadErr != nil && !errors.Is(loadErr, os.ErrNotExist) {
		return workerBootState{}, "", loadErr
	}
	next := workerBootState{Schema: workerBootSchemaV1, OSBootID: bootID, Generation: previous.Generation + 1, StartedAt: time.Now().UTC()}
	if err := writeWorkerBoot(path, next); err != nil {
		return workerBootState{}, "", err
	}
	return next, reason, nil
}

func operatingSystemBootID() (string, error) {
	if goruntime.GOOS == "linux" {
		body, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		value := strings.TrimSpace(string(body))
		if err != nil || value == "" || strings.ContainsAny(value, "\x00\r\n ") {
			return "", ErrProductionInvalid
		}
		return value, nil
	}
	output, err := exec.Command("/usr/sbin/sysctl", "-n", "kern.boottime").Output()
	value := strings.TrimSpace(string(output))
	if err != nil || value == "" || len(value) > 256 || strings.ContainsAny(value, "\x00\r\n") {
		return "", ErrProductionInvalid
	}
	return value, nil
}

func loadWorkerBoot(path string) (workerBootState, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return workerBootState{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || info.Size() < 1 || info.Size() > 4096 {
		return workerBootState{}, ErrProductionInvalid
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return workerBootState{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var state workerBootState
	var extra any
	if decoder.Decode(&state) != nil || decoder.Decode(&extra) != io.EOF || state.Schema != workerBootSchemaV1 || state.OSBootID == "" || state.Generation < 1 || state.StartedAt.IsZero() {
		return workerBootState{}, ErrProductionInvalid
	}
	return state, nil
}

func writeWorkerBoot(path string, state workerBootState) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrProductionInvalid
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".worker-boot-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(body); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
