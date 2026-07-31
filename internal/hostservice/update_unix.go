//go:build darwin || linux

package hostservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/binarytarget"
	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

var (
	ErrUpdateInvalid  = errors.New("privileged update is invalid")
	ErrUpdateRollback = errors.New("privileged update rollback failed")
)

const updateJournalSchemaV1 = "paperboat.host-update/v1"

type updateFetcher interface {
	Fetch(context.Context, bootstrap.ArtifactManifest, string, string) (string, error)
}

type updateServices interface {
	RestartWorker(context.Context) error
	RestartHost()
}

type updateHealth interface {
	Check(context.Context, string) error
}

type UpdateConfig struct {
	StateRoot      string
	WorkerPath     string
	HostPath       string
	PublicKey      string
	CurrentVersion string
	ListenAddress  string
}

type UpdateManager struct {
	mu        sync.Mutex
	config    UpdateConfig
	ownerUID  int
	fetcher   updateFetcher
	services  updateServices
	health    updateHealth
	journal   string
	current   string
	rollbacks string
}

type updateJournal struct {
	Schema          string    `json:"schema"`
	Stage           string    `json:"stage"`
	Version         string    `json:"version"`
	PreviousVersion string    `json:"previous_version"`
	WorkerStaged    string    `json:"worker_staged,omitempty"`
	HostStaged      string    `json:"host_staged,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func NewUpdateManager(config UpdateConfig) (*UpdateManager, error) {
	client := &http.Client{Timeout: 2 * time.Minute, CheckRedirect: secureUpdateRedirect}
	manager := &UpdateManager{
		config: config, ownerUID: 0, fetcher: artifactUpdateFetcher{client: client},
		services: platformUpdateServices{}, health: HTTPUpdateHealth{Address: config.ListenAddress},
		journal: filepath.Join(config.StateRoot, "update-journal.json"), current: filepath.Join(config.StateRoot, "update-current.json"), rollbacks: filepath.Join(config.StateRoot, "update-rollbacks.json"),
	}
	if err := manager.validate(); err != nil {
		return nil, err
	}
	if err := manager.recover(context.Background()); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *UpdateManager) Activate(ctx context.Context, worker, host bootstrap.ArtifactManifest) (string, error) {
	if os.Geteuid() != 0 {
		return "", ErrUpdateInvalid
	}
	return m.activate(ctx, worker, host)
}

func (m *UpdateManager) activate(ctx context.Context, worker, host bootstrap.ArtifactManifest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := verifyUpdatePair(m.config, worker, host); err != nil {
		return "", err
	}
	comparison := compareReleaseVersion(worker.Version, m.config.CurrentVersion)
	if comparison == 0 {
		entry, journalErr := m.loadJournal()
		if journalErr == nil && entry.Stage == "committed" && entry.Version == worker.Version && verifyInstalledArtifact(m.config.WorkerPath, worker, m.ownerUID) == nil && verifyInstalledArtifact(m.config.HostPath, host, m.ownerUID) == nil {
			return worker.Version, nil
		}
		return "", ErrUpdateInvalid
	}
	if comparison < 0 {
		return "", ErrUpdateInvalid
	}
	workerStaged, err := m.fetcher.Fetch(ctx, worker, m.config.PublicKey, filepath.Dir(m.config.WorkerPath))
	if err != nil {
		return "", err
	}
	defer os.Remove(workerStaged)
	hostStaged, err := m.fetcher.Fetch(ctx, host, m.config.PublicKey, filepath.Dir(m.config.HostPath))
	if err != nil {
		return "", err
	}
	defer os.Remove(hostStaged)
	if binarytarget.Validate(workerStaged, worker.Platform, worker.Architecture) != nil || binarytarget.Validate(hostStaged, host.Platform, host.Architecture) != nil {
		return "", ErrUpdateInvalid
	}
	entry := updateJournal{Schema: updateJournalSchemaV1, Stage: "staged", Version: worker.Version, PreviousVersion: m.config.CurrentVersion, WorkerStaged: workerStaged, HostStaged: hostStaged, UpdatedAt: time.Now().UTC()}
	if err := m.writeJournal(entry); err != nil {
		return "", err
	}
	entry.Stage, entry.UpdatedAt = "activating", time.Now().UTC()
	if err := m.writeJournal(entry); err != nil {
		return "", err
	}
	if err := replaceUpdateBinary(m.config.WorkerPath, workerStaged, m.ownerUID); err != nil {
		return "", errors.Join(err, m.rollback(ctx))
	}
	entry.WorkerStaged = ""
	if err := replaceUpdateBinary(m.config.HostPath, hostStaged, m.ownerUID); err != nil {
		return "", errors.Join(err, m.rollback(ctx))
	}
	entry.HostStaged, entry.Stage, entry.UpdatedAt = "", "checking", time.Now().UTC()
	if err := m.writeJournal(entry); err != nil {
		return "", errors.Join(err, m.rollback(ctx))
	}
	if err := m.services.RestartWorker(ctx); err != nil {
		return "", errors.Join(err, m.rollback(ctx))
	}
	if err := m.health.Check(ctx, worker.Version); err != nil {
		return "", errors.Join(err, m.rollback(ctx))
	}
	if err := m.writeCurrent(worker.Version); err != nil {
		return "", errors.Join(err, m.rollback(ctx))
	}
	entry.Stage, entry.UpdatedAt = "committed", time.Now().UTC()
	if err := m.writeJournal(entry); err != nil {
		return "", errors.Join(err, m.rollback(ctx))
	}
	if err := m.finalizePrevious(); err != nil {
		return "", err
	}
	m.config.CurrentVersion = worker.Version
	m.services.RestartHost()
	return worker.Version, nil
}

func (m *UpdateManager) validate() error {
	if !filepath.IsAbs(m.config.StateRoot) || !filepath.IsAbs(m.config.WorkerPath) || !filepath.IsAbs(m.config.HostPath) || m.config.WorkerPath == m.config.HostPath || m.config.PublicKey == "" || !validReleaseVersion(m.config.CurrentVersion) || m.config.ListenAddress == "" {
		return ErrUpdateInvalid
	}
	for _, path := range []string{m.config.StateRoot, filepath.Dir(m.config.WorkerPath), filepath.Dir(m.config.HostPath)} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || fileOwnerUID(info) != m.ownerUID {
			return ErrUpdateInvalid
		}
	}
	for _, path := range []string{m.config.WorkerPath, m.config.HostPath} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || fileOwnerUID(info) != m.ownerUID {
			return ErrUpdateInvalid
		}
	}
	return nil
}

func verifyUpdatePair(config UpdateConfig, worker, host bootstrap.ArtifactManifest) error {
	if bootstrap.VerifyArtifactManifest(worker, config.PublicKey) != nil || bootstrap.VerifyArtifactManifest(host, config.PublicKey) != nil || worker.Schema != bootstrap.ArtifactSchemaV1 || host.Schema != bootstrap.ArtifactSchemaV1 || worker.Kind != bootstrap.ArtifactKindWorker || host.Kind != bootstrap.ArtifactKindHostService || worker.Version != host.Version {
		return ErrUpdateInvalid
	}
	if worker.Platform != runtime.GOOS || host.Platform != runtime.GOOS || worker.Architecture != runtime.GOARCH || host.Architecture != runtime.GOARCH {
		return ErrUpdateInvalid
	}
	return nil
}

func verifyInstalledArtifact(path string, manifest bootstrap.ArtifactManifest, ownerUID int) error {
	if err := safeUpdateRegular(path, ownerUID); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, manifest.ByteLength+1))
	if err != nil || written != manifest.ByteLength || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), manifest.SHA256) {
		return ErrUpdateInvalid
	}
	return nil
}

func replaceUpdateBinary(current, staged string, ownerUID int) error {
	rollback := current + ".rollback"
	if err := safeUpdateRegular(current, ownerUID); err != nil {
		return err
	}
	if err := safeUpdateRegular(staged, ownerUID); err != nil {
		return err
	}
	if err := os.Chmod(staged, 0o755); err != nil {
		return err
	}
	if err := os.Remove(rollback); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(current, rollback); err != nil {
		return err
	}
	if err := os.Rename(staged, current); err != nil {
		_ = os.Rename(rollback, current)
		return err
	}
	return syncUpdateDirectory(filepath.Dir(current))
}

func (m *UpdateManager) rollback(ctx context.Context) error {
	var result error
	for _, current := range []string{m.config.WorkerPath, m.config.HostPath} {
		rollback := current + ".rollback"
		if _, err := os.Lstat(rollback); err == nil {
			if removeErr := os.Remove(current); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				result = errors.Join(result, removeErr)
				continue
			}
			if renameErr := os.Rename(rollback, current); renameErr != nil {
				result = errors.Join(result, renameErr)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	if result == nil {
		result = errors.Join(result, m.writeCurrent(m.config.CurrentVersion), m.services.RestartWorker(ctx), os.Remove(m.journal))
	}
	if result != nil {
		return fmt.Errorf("%w: %v", ErrUpdateRollback, result)
	}
	if err := m.incrementRollbackCount(); err != nil {
		return fmt.Errorf("%w: %v", ErrUpdateRollback, err)
	}
	return nil
}

func (m *UpdateManager) RollbackCount() uint64 {
	body, err := os.ReadFile(m.rollbacks)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	var value struct {
		Schema string `json:"schema"`
		Count  uint64 `json:"count"`
	}
	if err != nil || json.Unmarshal(body, &value) != nil || value.Schema != "paperboat.host-update-rollbacks/v1" {
		return 0
	}
	return value.Count
}

func (m *UpdateManager) incrementRollbackCount() error {
	value := struct {
		Schema string `json:"schema"`
		Count  uint64 `json:"count"`
	}{"paperboat.host-update-rollbacks/v1", m.RollbackCount() + 1}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return atomicRootWrite(m.rollbacks, body)
}

func (m *UpdateManager) recover(ctx context.Context) error {
	entry, err := m.loadJournal()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if entry.Stage == "committed" {
		if err := m.health.Check(ctx, entry.Version); err != nil {
			m.config.CurrentVersion = entry.PreviousVersion
			return errors.Join(err, m.rollback(ctx))
		}
		if err := m.finalizePrevious(); err != nil {
			return err
		}
		m.config.CurrentVersion = entry.Version
		return nil
	}
	for _, staged := range []string{entry.WorkerStaged, entry.HostStaged} {
		if staged != "" && (safeUpdateStaged(staged, filepath.Dir(m.config.WorkerPath)) || safeUpdateStaged(staged, filepath.Dir(m.config.HostPath))) {
			_ = os.Remove(staged)
		}
	}
	return m.rollback(ctx)
}

func (m *UpdateManager) finalizePrevious() error {
	for _, current := range []string{m.config.WorkerPath, m.config.HostPath} {
		rollback, previous := current+".rollback", current+".previous"
		if _, err := os.Lstat(rollback); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if err := os.Remove(previous); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Rename(rollback, previous); err != nil {
			return err
		}
		if err := syncUpdateDirectory(filepath.Dir(current)); err != nil {
			return err
		}
	}
	return nil
}

func (m *UpdateManager) writeJournal(entry updateJournal) error {
	body, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return atomicRootWrite(m.journal, body)
}

func (m *UpdateManager) loadJournal() (updateJournal, error) {
	body, err := os.ReadFile(m.journal)
	if err != nil {
		return updateJournal{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var entry updateJournal
	var extra any
	if decoder.Decode(&entry) != nil || decoder.Decode(&extra) != io.EOF || entry.Schema != updateJournalSchemaV1 || !validReleaseVersion(entry.Version) || !validReleaseVersion(entry.PreviousVersion) || compareReleaseVersion(entry.Version, entry.PreviousVersion) <= 0 || !containsString([]string{"staged", "activating", "checking", "committed"}, entry.Stage) {
		return updateJournal{}, ErrUpdateInvalid
	}
	return entry, nil
}

func (m *UpdateManager) writeCurrent(version string) error {
	body, _ := json.Marshal(struct {
		Schema  string `json:"schema"`
		Version string `json:"version"`
	}{"paperboat.host-update-current/v1", version})
	return atomicRootWrite(m.current, body)
}

type artifactUpdateFetcher struct{ client *http.Client }

func (f artifactUpdateFetcher) Fetch(ctx context.Context, manifest bootstrap.ArtifactManifest, key, directory string) (string, error) {
	return bootstrap.FetchVerifiedArtifact(ctx, manifest, key, directory, f.client)
}

type platformUpdateServices struct{}

func (platformUpdateServices) RestartWorker(ctx context.Context) error {
	if runtime.GOOS == "linux" {
		return exec.CommandContext(ctx, "/usr/bin/systemctl", "restart", "paperboat-helper.service").Run()
	}
	return exec.CommandContext(ctx, "/bin/launchctl", "kickstart", "-k", "system/com.pinksaucepasta.paperboat-helper").Run()
}

func (platformUpdateServices) RestartHost() {
	go func() {
		// Let the activation response flush before this process replaces itself.
		time.Sleep(2 * time.Second)
		if runtime.GOOS == "linux" {
			_ = exec.Command("/usr/bin/systemctl", "restart", "--no-block", "paperboat-host-service.service").Run()
			return
		}
		_ = exec.Command("/bin/launchctl", "kickstart", "-k", "system/com.pinksaucepasta.paperboat-host-service").Run()
	}()
}

type HTTPUpdateHealth struct{ Address string }

func (h HTTPUpdateHealth) Check(ctx context.Context, version string) error {
	deadline, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		request, _ := http.NewRequestWithContext(deadline, http.MethodGet, "http://"+h.Address+"/healthz", nil)
		response, err := client.Do(request)
		if err == nil {
			var value struct {
				Live    bool   `json:"live"`
				Version string `json:"version"`
			}
			decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&value)
			response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && value.Live && value.Version == version {
				return nil
			}
		}
		select {
		case <-deadline.Done():
			return deadline.Err()
		case <-time.After(time.Second):
		}
	}
}

func secureUpdateRedirect(request *http.Request, via []*http.Request) error {
	if len(via) > 5 || request.URL.Scheme != "https" || request.URL.User != nil || request.URL.Hostname() == "" {
		return ErrUpdateInvalid
	}
	return nil
}

func atomicRootWrite(path string, body []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".paperboat-update-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
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
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncUpdateDirectory(filepath.Dir(path))
}

func safeUpdateRegular(path string, ownerUID int) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || fileOwnerUID(info) != ownerUID {
		return ErrUpdateInvalid
	}
	return nil
}

func fileOwnerUID(info os.FileInfo) int {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return -1
	}
	return int(stat.Uid)
}

func safeUpdateStaged(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && strings.HasPrefix(filepath.Base(path), ".paperboat-helper-artifact-")
}

func syncUpdateDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func validReleaseVersion(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) < 3 || len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return false
		}
	}
	return true
}

func compareReleaseVersion(left, right string) int {
	l, r := strings.Split(left, "."), strings.Split(right, ".")
	for index := range 4 {
		var lv, rv uint64
		if index < len(l) {
			lv, _ = strconv.ParseUint(l[index], 10, 32)
		}
		if index < len(r) {
			rv, _ = strconv.ParseUint(r[index], 10, 32)
		}
		if lv < rv {
			return -1
		}
		if lv > rv {
			return 1
		}
	}
	return 0
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
