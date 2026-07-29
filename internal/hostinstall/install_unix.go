//go:build darwin || linux

package hostinstall

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/binarytarget"
	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
	"github.com/pinksaucepasta/paperboat-helper/internal/hostservice"
	"github.com/pinksaucepasta/paperboat-helper/internal/service"
)

const SchemaV1 = "paperboat.host-install/v1"

const journalSchemaV1 = "paperboat.host-install-journal/v1"

var (
	ErrInvalidRequest = errors.New("invalid privileged installation request")
	ErrNotPrivileged  = errors.New("privileged installation requires administrator approval")
)

// Request is the complete allowlist accepted by the privileged installer.
// It deliberately has no generic command, argument, path, or environment fields.
type Request struct {
	Schema              string                     `json:"schema"`
	Platform            string                     `json:"platform"`
	User                string                     `json:"user"`
	UID                 int                        `json:"uid"`
	Group               string                     `json:"group"`
	GID                 int                        `json:"gid"`
	Executable          string                     `json:"executable"`
	Artifact            bootstrap.ArtifactManifest `json:"artifact"`
	HostExecutable      string                     `json:"host_executable"`
	HostArtifact        bootstrap.ArtifactManifest `json:"host_artifact"`
	ArtifactPublicKey   string                     `json:"artifact_public_key"`
	Home                string                     `json:"home"`
	Path                string                     `json:"path"`
	StateRoot           string                     `json:"state_root"`
	WorkspaceRoot       string                     `json:"workspace_root"`
	ControlURL          string                     `json:"control_url"`
	UserMachineID       string                     `json:"user_machine_id"`
	Shell               string                     `json:"shell"`
	HelperListenAddress string                     `json:"helper_listen_address"`
}

func Decode(reader io.Reader) (Request, error) {
	var request Request
	decoder := json.NewDecoder(io.LimitReader(reader, 128<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return Request{}, ErrInvalidRequest
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return Request{}, ErrInvalidRequest
	}
	return request, nil
}

func Install(ctx context.Context, request Request) error {
	if os.Geteuid() != 0 {
		return ErrNotPrivileged
	}
	if err := Validate(request, invokingUID()); err != nil {
		return err
	}
	paths := platformPaths()
	if err := recoverInterrupted(ctx, request, paths); err != nil {
		return err
	}
	if err := stageBinary(request.Executable, paths.workerNext, request.Artifact); err != nil {
		return err
	}
	if err := stageBinary(request.HostExecutable, paths.hostNext, request.HostArtifact); err != nil {
		return err
	}
	journal := installJournal{Schema: journalSchemaV1, Stage: "prepared", HadWorker: regularFile(paths.worker), HadHost: regularFile(paths.host), UpdatedAt: time.Now().UTC()}
	if err := writeJournal(paths.journal, journal); err != nil {
		return err
	}
	if err := activateBinary(paths.worker, paths.workerNext, paths.workerRollback); err != nil {
		return errors.Join(err, rollbackFiles(paths, journal))
	}
	if err := activateBinary(paths.host, paths.hostNext, paths.hostRollback); err != nil {
		return errors.Join(err, rollbackFiles(paths, journal))
	}
	journal.Stage, journal.UpdatedAt = "binaries_activated", time.Now().UTC()
	if err := writeJournal(paths.journal, journal); err != nil {
		return errors.Join(err, rollbackFiles(paths, journal))
	}
	worker, host, err := installers(request, paths)
	if err != nil {
		return errors.Join(ErrInvalidRequest, err)
	}
	if err := host.Install(ctx); err != nil {
		return errors.Join(err, rollbackFiles(paths, journal))
	}
	legacy, err := inspectLegacyService(ctx, request)
	if err != nil {
		return errors.Join(err, host.Uninstall(ctx), rollbackFiles(paths, journal))
	}
	journal.HadLegacy, journal.LegacyActive, journal.LegacyEnabled = legacy.Exists, legacy.Active, legacy.Enabled
	if legacy.Active {
		if err := stopLegacyService(ctx, request); err != nil {
			return errors.Join(err, host.Uninstall(ctx), rollbackFiles(paths, journal))
		}
		journal.LegacyStopped = true
		if err := writeJournal(paths.journal, journal); err != nil {
			return errors.Join(err, restartLegacyService(ctx, request, journal), host.Uninstall(ctx), rollbackFiles(paths, journal))
		}
	}
	if err := worker.Install(ctx); err != nil {
		return errors.Join(err, restartLegacyService(ctx, request, journal), host.Uninstall(ctx), rollbackFiles(paths, journal))
	}
	journal.Stage, journal.UpdatedAt = "services_started", time.Now().UTC()
	return writeJournal(paths.journal, journal)
}

func Commit(request Request) error {
	if os.Geteuid() != 0 {
		return ErrNotPrivileged
	}
	if err := Validate(request, invokingUID()); err != nil {
		return err
	}
	paths := platformPaths()
	journal, err := loadJournal(paths.journal)
	if err != nil || journal.Stage != "services_started" {
		return ErrInvalidRequest
	}
	for _, pair := range [][2]string{{paths.workerRollback, paths.workerPrevious}, {paths.hostRollback, paths.hostPrevious}} {
		if err := replacePrevious(pair[0], pair[1]); err != nil {
			return err
		}
	}
	if err := writeInstallMetadata(paths.metadata, request); err != nil {
		return err
	}
	if journal.HadLegacy {
		if err := removeLegacyService(request); err != nil {
			return err
		}
	}
	return removeJournal(paths.journal)
}

func Uninstall(ctx context.Context, request Request) error {
	if os.Geteuid() != 0 {
		return ErrNotPrivileged
	}
	if err := Validate(request, invokingUID()); err != nil {
		return err
	}
	return uninstallValidated(ctx, request, platformPaths())
}

func UninstallPersisted(ctx context.Context) error {
	if os.Geteuid() != 0 {
		return ErrNotPrivileged
	}
	paths := platformPaths()
	request, err := loadInstallMetadata(paths.metadata, invokingUID())
	if err != nil {
		return err
	}
	return uninstallValidated(ctx, request, paths)
}

func uninstallValidated(ctx context.Context, request Request, paths installPaths) error {
	worker, host, err := installers(request, paths)
	if err != nil {
		return errors.Join(ErrInvalidRequest, err)
	}
	serviceErr := errors.Join(worker.Uninstall(ctx), host.Uninstall(ctx))
	restoreErr := hostservice.NewPlatformApplier(filepath.Join(paths.state, "power-baseline.json")).Apply(ctx, hostservice.AllowSleep)
	journal, journalErr := loadJournal(paths.journal)
	if journalErr == nil {
		return errors.Join(serviceErr, restoreErr, restartLegacyService(ctx, request, journal), rollbackFiles(paths, journal))
	}
	if !errors.Is(journalErr, os.ErrNotExist) {
		return errors.Join(serviceErr, restoreErr, journalErr)
	}
	return errors.Join(serviceErr, restoreErr, removeInstalledFiles(paths))
}

func installers(request Request, paths installPaths) (*service.Installer, *service.Installer, error) {
	workerController := service.Controller(service.SystemdController{Runner: service.ExecRunner{}})
	hostController := service.Controller(service.SystemdController{Runner: service.ExecRunner{}, Unit: "paperboat-host-service.service"})
	rootGroup := "root"
	if runtime.GOOS == "darwin" {
		rootGroup = "wheel"
		workerController = service.LaunchdController{Runner: service.ExecRunner{}, UID: request.UID}
		hostController = service.LaunchdController{Runner: service.ExecRunner{}, UID: request.UID, Label: service.HostLabel}
	}
	worker, err := service.New(service.Config{
		Platform: request.Platform, Kind: service.WorkerKind, ConfigRoot: string(os.PathSeparator), Executable: paths.worker,
		User: request.User, Group: request.Group, Arguments: []string{"run"}, Controller: workerController,
		Environment: workerEnvironment(request),
	})
	if err != nil {
		return nil, nil, err
	}
	host, err := service.New(service.Config{
		Platform: request.Platform, Kind: service.HostKind, ConfigRoot: string(os.PathSeparator), Executable: paths.host,
		User: "root", Group: rootGroup, Arguments: []string{
			"--uid", strconv.Itoa(request.UID), "--gid", strconv.Itoa(request.GID),
			"--artifact-public-key", request.ArtifactPublicKey, "--listen-address", request.HelperListenAddress,
		}, Controller: hostController,
	})
	if err != nil {
		return nil, nil, err
	}
	return worker, host, nil
}

type installPaths struct{ root, state, worker, host, workerNext, hostNext, workerRollback, hostRollback, workerPrevious, hostPrevious, journal, metadata string }
type installJournal struct {
	Schema        string    `json:"schema"`
	Stage         string    `json:"stage"`
	HadWorker     bool      `json:"had_worker"`
	HadHost       bool      `json:"had_host"`
	HadLegacy     bool      `json:"had_legacy,omitempty"`
	LegacyActive  bool      `json:"legacy_active,omitempty"`
	LegacyEnabled bool      `json:"legacy_enabled,omitempty"`
	LegacyStopped bool      `json:"legacy_stopped,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type legacyServiceState struct {
	Exists  bool
	Active  bool
	Enabled bool
}

func legacyServicePath(request Request) string {
	if runtime.GOOS == "darwin" {
		return filepath.Join(request.Home, "Library", "LaunchAgents", service.Label+".plist")
	}
	return filepath.Join(request.Home, ".config", "systemd", "user", "paperboat-helper.service")
}

func inspectLegacyService(ctx context.Context, request Request) (legacyServiceState, error) {
	path := legacyServicePath(request)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return legacyServiceState{}, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || ownerUID(info) != request.UID {
		return legacyServiceState{}, ErrInvalidRequest
	}
	state := legacyServiceState{Exists: true, Enabled: true}
	if runtime.GOOS == "linux" {
		state.Active = runLegacySystemctl(ctx, request, "is-active") == nil
		state.Enabled = runLegacySystemctl(ctx, request, "is-enabled") == nil
		return state, nil
	}
	command := exec.CommandContext(ctx, "/bin/launchctl", "print", fmt.Sprintf("gui/%d/%s", request.UID, service.Label))
	state.Active = command.Run() == nil
	return state, nil
}

func stopLegacyService(ctx context.Context, request Request) error {
	if runtime.GOOS == "linux" {
		return runLegacySystemctl(ctx, request, "stop")
	}
	command := exec.CommandContext(ctx, "/bin/launchctl", "bootout", fmt.Sprintf("gui/%d/%s", request.UID, service.Label))
	return command.Run()
}

func restartLegacyService(ctx context.Context, request Request, journal installJournal) error {
	if !journal.HadLegacy || !journal.LegacyActive || !journal.LegacyStopped {
		return nil
	}
	if runtime.GOOS == "linux" {
		return runLegacySystemctl(ctx, request, "start")
	}
	command := exec.CommandContext(ctx, "/bin/launchctl", "bootstrap", fmt.Sprintf("gui/%d", request.UID), legacyServicePath(request))
	return command.Run()
}

func removeLegacyService(request Request) error {
	path := legacyServicePath(request)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || ownerUID(info) != request.UID {
		return ErrInvalidRequest
	}
	if runtime.GOOS == "linux" {
		_ = runLegacySystemctl(context.Background(), request, "disable")
	}
	return os.Remove(path)
}

func runLegacySystemctl(ctx context.Context, request Request, operation string) error {
	if !slices.Contains([]string{"is-active", "is-enabled", "stop", "start", "disable"}, operation) {
		return ErrInvalidRequest
	}
	command := exec.CommandContext(ctx, "/usr/bin/systemctl", "--user", "--machine="+request.User+"@", operation, "paperboat-helper.service")
	return command.Run()
}

func platformPaths() installPaths {
	root, state := "/usr/local/libexec/paperboat", "/var/lib/paperboat"
	if runtime.GOOS == "darwin" {
		root, state = "/Library/PrivilegedHelperTools/Paperboat", "/Library/Application Support/Paperboat"
	}
	p := installPaths{root: root, state: state}
	p.worker, p.host = filepath.Join(root, "pbh"), filepath.Join(root, "paperboat-host-service")
	p.workerNext, p.hostNext = p.worker+".next", p.host+".next"
	p.workerRollback, p.hostRollback = p.worker+".rollback", p.host+".rollback"
	p.workerPrevious, p.hostPrevious = p.worker+".previous", p.host+".previous"
	p.journal = filepath.Join(state, "install-journal.json")
	p.metadata = filepath.Join(state, "install-metadata.json")
	return p
}

func writeInstallMetadata(path string, request Request) error {
	if err := secureRootDirectory(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".install-metadata-*")
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

func loadInstallMetadata(path string, sudoUID int) (Request, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || ownerUID(info) != 0 || info.Size() < 1 || info.Size() > 128<<10 {
		return Request{}, ErrInvalidRequest
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return Request{}, err
	}
	request, err := Decode(strings.NewReader(string(body)))
	if err != nil || request.Schema != SchemaV1 || request.Platform != runtime.GOOS || !validRunIdentity(request) || request.UID != sudoUID {
		return Request{}, ErrInvalidRequest
	}
	account, err := user.Lookup(request.User)
	if err != nil || account.Uid != strconv.Itoa(request.UID) || account.Gid != strconv.Itoa(request.GID) || account.HomeDir != request.Home {
		return Request{}, ErrInvalidRequest
	}
	group, err := user.LookupGroup(request.Group)
	if err != nil || group.Gid != strconv.Itoa(request.GID) {
		return Request{}, ErrInvalidRequest
	}
	return request, nil
}

func stageBinary(source, destination string, manifest bootstrap.ArtifactManifest) error {
	if err := secureRootDirectory(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".binary-*")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer os.Remove(path)
	if err := temporary.Chmod(0o755); err != nil {
		temporary.Close()
		return err
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(input, manifest.ByteLength+1))
	if err != nil || written != manifest.ByteLength || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), manifest.SHA256) {
		temporary.Close()
		return ErrInvalidRequest
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(path, destination); err != nil {
		return err
	}
	return nil
}

func activateBinary(current, next, rollback string) error {
	if err := os.Remove(rollback); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if info, err := os.Lstat(current); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || ownerUID(info) != 0 {
			return ErrInvalidRequest
		}
		if err := os.Rename(current, rollback); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(next, current)
}

func rollbackFiles(paths installPaths, journal installJournal) error {
	var result error
	for _, item := range []struct {
		current, rollback string
		had               bool
	}{{paths.worker, paths.workerRollback, journal.HadWorker}, {paths.host, paths.hostRollback, journal.HadHost}} {
		rollbackExists := regularFile(item.rollback)
		if rollbackExists || !item.had {
			if err := os.Remove(item.current); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
		if item.had && rollbackExists {
			if err := os.Rename(item.rollback, item.current); err != nil {
				result = errors.Join(result, err)
			}
		} else if !item.had {
			_ = os.Remove(item.rollback)
		}
	}
	_ = os.Remove(paths.workerNext)
	_ = os.Remove(paths.hostNext)
	return errors.Join(result, removeJournal(paths.journal))
}

func recoverInterrupted(ctx context.Context, request Request, paths installPaths) error {
	journal, err := loadJournal(paths.journal)
	if errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(paths.workerNext)
		_ = os.Remove(paths.hostNext)
		return nil
	}
	if err != nil {
		return err
	}
	if regularFile(paths.worker) && regularFile(paths.host) {
		if worker, host, installErr := installers(request, paths); installErr == nil {
			_ = worker.Uninstall(ctx)
			_ = host.Uninstall(ctx)
		}
	}
	return errors.Join(restartLegacyService(ctx, request, journal), rollbackFiles(paths, journal))
}

func writeJournal(path string, journal installJournal) error {
	if err := secureRootDirectory(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".journal-*")
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
	return os.Rename(name, path)
}

func loadJournal(path string) (installJournal, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return installJournal{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > 4096 || ownerUID(info) != 0 {
		return installJournal{}, ErrInvalidRequest
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return installJournal{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	var journal installJournal
	if decoder.Decode(&journal) != nil || journal.Schema != journalSchemaV1 || !slices.Contains([]string{"prepared", "binaries_activated", "services_started"}, journal.Stage) {
		return installJournal{}, ErrInvalidRequest
	}
	return journal, nil
}

func replacePrevious(rollback, previous string) error {
	if _, err := os.Lstat(rollback); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := os.Remove(previous); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(rollback, previous)
}
func removeJournal(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
func removeInstalledFiles(paths installPaths) error {
	var result error
	for _, path := range []string{
		paths.worker, paths.host, paths.workerNext, paths.hostNext,
		paths.workerRollback, paths.hostRollback, paths.workerPrevious, paths.hostPrevious,
		paths.metadata,
		filepath.Join(paths.state, "power-baseline.json"),
		filepath.Join(paths.state, "availability-policy.json"),
		filepath.Join(paths.state, "update-current.json"),
		filepath.Join(paths.state, "update-journal.json"),
		filepath.Join(paths.state, "update-rollbacks.json"),
	} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}
func regularFile(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0
}
func secureRootDirectory(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || ownerUID(info) != 0 {
		return ErrInvalidRequest
	}
	return os.Chmod(path, mode)
}

func Validate(request Request, sudoUID int) error {
	if request.Schema != SchemaV1 || request.Platform != runtime.GOOS || !validRunIdentity(request) || sudoUID != request.UID ||
		request.UserMachineID == "" || strings.ContainsAny(request.UserMachineID, "\x00\r\n") {
		return ErrInvalidRequest
	}
	account, err := user.Lookup(request.User)
	if err != nil || account.Uid != strconv.Itoa(request.UID) || account.Gid != strconv.Itoa(request.GID) || account.HomeDir != request.Home {
		return ErrInvalidRequest
	}
	group, err := user.LookupGroup(request.Group)
	if err != nil || group.Gid != strconv.Itoa(request.GID) {
		return ErrInvalidRequest
	}
	if err := bootstrap.VerifyArtifactManifest(request.Artifact, request.ArtifactPublicKey); err != nil ||
		bootstrap.VerifyArtifactManifest(request.HostArtifact, request.ArtifactPublicKey) != nil || request.Artifact.Platform != request.Platform ||
		request.Artifact.Schema != bootstrap.ArtifactSchemaV2 || request.Artifact.Kind != bootstrap.ArtifactKindWorker ||
		request.HostArtifact.Schema != bootstrap.ArtifactSchemaV2 || request.HostArtifact.Kind != bootstrap.ArtifactKindHostService ||
		request.Artifact.Version != request.HostArtifact.Version || request.Artifact.Architecture != request.HostArtifact.Architecture ||
		verifyArtifact(request.Executable, request.Artifact, request.UID) != nil || verifyArtifact(request.HostExecutable, request.HostArtifact, request.UID) != nil ||
		binarytarget.Validate(request.Executable, request.Platform, request.Artifact.Architecture) != nil ||
		binarytarget.Validate(request.HostExecutable, request.Platform, request.HostArtifact.Architecture) != nil {
		return ErrInvalidRequest
	}
	for _, path := range []string{request.Home, request.StateRoot, request.WorkspaceRoot} {
		if !canonicalOwnedDirectory(path, request.UID) {
			return ErrInvalidRequest
		}
	}
	if !canonicalExecutable(request.Shell) {
		return ErrInvalidRequest
	}
	if !pathListValid(request.Path) {
		return ErrInvalidRequest
	}
	parsed, err := url.Parse(request.ControlURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return ErrInvalidRequest
	}
	host, port, err := net.SplitHostPort(request.HelperListenAddress)
	if err != nil || port == "" || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return ErrInvalidRequest
	}
	return nil
}

func validRunIdentity(request Request) bool {
	return request.UID > 0 && request.GID > 0 || request.UID == 0 && request.GID == 0 && request.User == "root"
}

func workerEnvironment(request Request) map[string]string {
	return map[string]string{
		"HOME": request.Home, "PATH": request.Path, "PAPERBOAT_HELPER_PROFILE": "byod",
		"PAPERBOAT_HELPER_STATE_ROOT": request.StateRoot, "PAPERBOAT_WORKSPACE_ROOT": request.WorkspaceRoot,
		"PAPERBOAT_CONTROL_URL": request.ControlURL, "PAPERBOAT_USER_MACHINE_ID": request.UserMachineID,
		"PAPERBOAT_SHELL": request.Shell, "PAPERBOAT_HELPER_LISTEN_ADDRESS": request.HelperListenAddress,
		"PAPERBOAT_HELPER_SERVICE_SCOPE": "system",
	}
}

func invokingUID() int {
	uid, err := strconv.Atoi(os.Getenv("SUDO_UID"))
	if err != nil {
		return -1
	}
	return uid
}

func verifyArtifact(path string, manifest bootstrap.ArtifactManifest, uid int) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrInvalidRequest
	}
	lstat, err := os.Lstat(path)
	if err != nil || !lstat.Mode().IsRegular() || lstat.Mode()&os.ModeSymlink != 0 || lstat.Mode().Perm()&0o022 != 0 || ownerUID(lstat) != uid {
		return ErrInvalidRequest
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != manifest.ByteLength || ownerUID(info) != uid {
		return ErrInvalidRequest
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, manifest.ByteLength+1)); err != nil || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), manifest.SHA256) {
		return ErrInvalidRequest
	}
	return nil
}

func canonicalOwnedDirectory(path string, uid int) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0o022 == 0 && ownerUID(info) == uid
}

func canonicalExecutable(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0o111 != 0 && info.Mode().Perm()&0o022 == 0
}

func pathListValid(value string) bool {
	if value == "" || strings.ContainsAny(value, "\x00\r\n") {
		return false
	}
	for _, path := range filepath.SplitList(value) {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return false
		}
	}
	return true
}

func ownerUID(info os.FileInfo) int {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return -1
	}
	return int(stat.Uid)
}

func (r Request) String() string {
	return fmt.Sprintf("%s for uid %d", r.Schema, r.UID)
}
