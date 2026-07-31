//go:build darwin || linux

package hostservice

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

type fakeUpdateFetcher struct{ bodies map[string][]byte }

func (f fakeUpdateFetcher) Fetch(_ context.Context, manifest bootstrap.ArtifactManifest, _ string, directory string) (string, error) {
	file, err := os.CreateTemp(directory, ".paperboat-helper-artifact-")
	if err != nil {
		return "", err
	}
	if err := file.Chmod(0o700); err != nil {
		file.Close()
		return "", err
	}
	_, err = file.Write(f.bodies[manifest.Kind])
	closeErr := file.Close()
	return file.Name(), errors.Join(err, closeErr)
}

type fakeUpdateServices struct{ workerRestarts, hostRestarts int }

func (s *fakeUpdateServices) RestartWorker(context.Context) error { s.workerRestarts++; return nil }
func (s *fakeUpdateServices) RestartHost()                        { s.hostRestarts++ }

type fakeUpdateHealth struct{ err error }

func (h fakeUpdateHealth) Check(context.Context, string) error { return h.err }

func TestRootUpdateActivatesPairedArtifactsAndPreservesPrevious(t *testing.T) {
	manager, worker, host := testUpdateManager(t, nil)
	version, err := manager.activate(context.Background(), worker, host)
	if err != nil || version != "2026.07.27" {
		t.Fatalf("activate version=%q err=%v", version, err)
	}
	assertFileBody(t, manager.config.WorkerPath, string(testBinary("new-worker")))
	assertFileBody(t, manager.config.HostPath, string(testBinary("new-host")))
	for _, path := range []string{manager.config.WorkerPath, manager.config.HostPath} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if info.Mode().Perm() != 0o755 {
			t.Fatalf("installed mode for %s = %v", path, info.Mode().Perm())
		}
	}
	assertFileBody(t, manager.config.WorkerPath+".previous", string(testBinary("old-worker")))
	assertFileBody(t, manager.config.HostPath+".previous", string(testBinary("old-host")))
	services := manager.services.(*fakeUpdateServices)
	if services.workerRestarts != 1 || services.hostRestarts != 1 {
		t.Fatalf("restarts=%+v", services)
	}
	entry, err := manager.loadJournal()
	if err != nil || entry.Stage != "committed" || entry.Version != version {
		t.Fatalf("journal=%+v err=%v", entry, err)
	}
	replayed, err := manager.activate(context.Background(), worker, host)
	if err != nil || replayed != version || services.workerRestarts != 1 || services.hostRestarts != 1 {
		t.Fatalf("replay version=%q err=%v restarts=%+v", replayed, err, services)
	}
}

func TestRootUpdateHealthFailureRollsBackBothBinaries(t *testing.T) {
	manager, worker, host := testUpdateManager(t, errors.New("new worker unhealthy"))
	if _, err := manager.activate(context.Background(), worker, host); err == nil {
		t.Fatal("health failure was accepted")
	}
	assertFileBody(t, manager.config.WorkerPath, string(testBinary("old-worker")))
	assertFileBody(t, manager.config.HostPath, string(testBinary("old-host")))
	if _, err := os.Stat(manager.journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal error=%v", err)
	}
	services := manager.services.(*fakeUpdateServices)
	if services.workerRestarts != 2 || services.hostRestarts != 0 {
		t.Fatalf("restarts=%+v", services)
	}
	if count := manager.RollbackCount(); count != 1 {
		t.Fatalf("rollback count=%d", count)
	}
}

func TestRootUpdateRejectsSignedBinaryForDifferentTarget(t *testing.T) {
	manager, _, _ := testUpdateManager(t, nil)
	foreign := make([]byte, 32)
	if runtime.GOOS == "linux" {
		binary.LittleEndian.PutUint32(foreign[:4], 0xfeedfacf)
		binary.LittleEndian.PutUint32(foreign[4:8], 0x0100000c)
	} else {
		copy(foreign, "\x7fELF")
		foreign[4], foreign[5] = 2, 1
		binary.LittleEndian.PutUint16(foreign[18:20], 62)
	}
	worker, host, publicKey := signedUpdatePair(t, foreign, foreign)
	manager.config.PublicKey = publicKey
	manager.fetcher = fakeUpdateFetcher{bodies: map[string][]byte{bootstrap.ArtifactKindWorker: foreign, bootstrap.ArtifactKindHostService: foreign}}
	if _, err := manager.activate(context.Background(), worker, host); !errors.Is(err, ErrUpdateInvalid) {
		t.Fatalf("foreign binary error=%v", err)
	}
	assertFileBody(t, manager.config.WorkerPath, string(testBinary("old-worker")))
	assertFileBody(t, manager.config.HostPath, string(testBinary("old-host")))
}

func TestRootUpdateRecoveryRollsBackInterruptedActivation(t *testing.T) {
	manager, _, _ := testUpdateManager(t, nil)
	for _, item := range []struct{ path, old, next string }{
		{manager.config.WorkerPath, string(testBinary("old-worker")), string(testBinary("new-worker"))},
		{manager.config.HostPath, string(testBinary("old-host")), string(testBinary("new-host"))},
	} {
		if err := os.Rename(item.path, item.path+".rollback"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(item.path, []byte(item.next), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	entry := updateJournal{Schema: updateJournalSchemaV1, Stage: "checking", Version: "2026.07.27", PreviousVersion: "2026.07.26", UpdatedAt: time.Now().UTC()}
	if err := manager.writeJournal(entry); err != nil {
		t.Fatal(err)
	}
	if err := manager.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertFileBody(t, manager.config.WorkerPath, string(testBinary("old-worker")))
	assertFileBody(t, manager.config.HostPath, string(testBinary("old-host")))
	if count := manager.RollbackCount(); count != 1 {
		t.Fatalf("recovery rollback count=%d", count)
	}
}

func TestReleaseVersionComparisonIncludesRevision(t *testing.T) {
	for _, version := range []string{"2026.07.25", "2026.07.25.4"} {
		if !validReleaseVersion(version) {
			t.Fatalf("valid version rejected: %s", version)
		}
	}
	for _, version := range []string{"2026.07", "2026.07.25.4.1", "2026.07.x"} {
		if validReleaseVersion(version) {
			t.Fatalf("invalid version accepted: %s", version)
		}
	}
	if compareReleaseVersion("2026.07.25.5", "2026.07.25.4") <= 0 || compareReleaseVersion("2026.07.25", "2026.07.25.1") >= 0 {
		t.Fatal("release revision was not compared")
	}
}

func testUpdateManager(t *testing.T, healthErr error) (*UpdateManager, bootstrap.ArtifactManifest, bootstrap.ArtifactManifest) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	install := filepath.Join(root, "install")
	state := filepath.Join(root, "state")
	for _, directory := range []string{install, state} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	workerPath, hostPath := filepath.Join(install, "pbh"), filepath.Join(install, "paperboat-host-service")
	if err := os.WriteFile(workerPath, testBinary("old-worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hostPath, testBinary("old-host"), 0o700); err != nil {
		t.Fatal(err)
	}
	workerBody, hostBody := testBinary("new-worker"), testBinary("new-host")
	worker, host, publicKey := signedUpdatePair(t, workerBody, hostBody)
	manager := &UpdateManager{
		config:   UpdateConfig{StateRoot: state, WorkerPath: workerPath, HostPath: hostPath, PublicKey: publicKey, CurrentVersion: "2026.07.26", ListenAddress: "127.0.0.1:8080"},
		ownerUID: os.Getuid(), fetcher: fakeUpdateFetcher{bodies: map[string][]byte{bootstrap.ArtifactKindWorker: workerBody, bootstrap.ArtifactKindHostService: hostBody}},
		services: &fakeUpdateServices{}, health: fakeUpdateHealth{err: healthErr}, journal: filepath.Join(state, "update-journal.json"), current: filepath.Join(state, "update-current.json"), rollbacks: filepath.Join(state, "update-rollbacks.json"),
	}
	if err := manager.validate(); err != nil {
		t.Fatal(err)
	}
	return manager, worker, host
}

func testBinary(label string) []byte {
	header := make([]byte, 32)
	if runtime.GOOS == "linux" {
		copy(header, "\x7fELF")
		header[4], header[5] = 2, 1
		machine := uint16(62)
		if runtime.GOARCH == "arm64" {
			machine = 183
		}
		binary.LittleEndian.PutUint16(header[18:20], machine)
	} else {
		binary.LittleEndian.PutUint32(header[:4], 0xfeedfacf)
		cpu := uint32(0x01000007)
		if runtime.GOARCH == "arm64" {
			cpu = 0x0100000c
		}
		binary.LittleEndian.PutUint32(header[4:8], cpu)
	}
	return append(header, label...)
}

func signedUpdatePair(t *testing.T, workerBody, hostBody []byte) (bootstrap.ArtifactManifest, bootstrap.ArtifactManifest, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(kind string, body []byte) bootstrap.ArtifactManifest {
		digest := sha256.Sum256(body)
		manifest := bootstrap.ArtifactManifest{Schema: bootstrap.ArtifactSchemaV1, Kind: kind, Version: "2026.07.27", Platform: runtime.GOOS, Architecture: runtime.GOARCH, URL: "https://updates.example.test/" + kind, ByteLength: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
		payload, _ := json.Marshal(struct {
			Architecture string `json:"architecture"`
			ByteLength   int64  `json:"byte_length"`
			Kind         string `json:"kind"`
			Platform     string `json:"platform"`
			Schema       string `json:"schema"`
			SHA256       string `json:"sha256"`
			URL          string `json:"url"`
			Version      string `json:"version"`
		}{manifest.Architecture, manifest.ByteLength, manifest.Kind, manifest.Platform, manifest.Schema, manifest.SHA256, manifest.URL, manifest.Version})
		manifest.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
		return manifest
	}
	return sign(bootstrap.ArtifactKindWorker, workerBody), sign(bootstrap.ArtifactKindHostService, hostBody), base64.RawURLEncoding.EncodeToString(publicKey)
}

func assertFileBody(t *testing.T, path, expected string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil || string(body) != expected {
		t.Fatalf("%s body=%q err=%v", path, body, err)
	}
}
