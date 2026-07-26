//go:build darwin || linux

package hostinstall

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

func TestDecodeRejectsUnknownAndTrailingFields(t *testing.T) {
	for _, input := range []string{
		`{"schema":"paperboat.host-install/v1","command":"sh"}`,
		`{"schema":"paperboat.host-install/v1"} {}`,
	} {
		if _, err := Decode(bytes.NewBufferString(input)); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("input accepted: %s: %v", input, err)
		}
	}
}

func TestRemoveInstalledFilesDeletesOnlyAllowlistedHostState(t *testing.T) {
	root, state := filepath.Join(t.TempDir(), "install"), filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := installPaths{root: root, state: state, worker: filepath.Join(root, "pbh"), host: filepath.Join(root, "paperboat-host-service"), metadata: filepath.Join(state, "install-metadata.json")}
	removed := []string{paths.worker, paths.host, paths.metadata, filepath.Join(state, "power-baseline.json"), filepath.Join(state, "availability-policy.json"), filepath.Join(state, "update-current.json"), filepath.Join(state, "update-journal.json"), filepath.Join(state, "update-rollbacks.json")}
	for _, path := range removed {
		if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := filepath.Join(state, "unrelated")
	if err := os.WriteFile(unrelated, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeInstalledFiles(paths); err != nil {
		t.Fatal(err)
	}
	for _, path := range removed {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("allowlisted path retained: %s (%v)", path, err)
		}
	}
	if body, err := os.ReadFile(unrelated); err != nil || string(body) != "preserve" {
		t.Fatalf("unrelated file changed: %q, %v", body, err)
	}
}

func TestValidateBindsSignedArtifactAndInvokingUID(t *testing.T) {
	request := validRequest(t)
	if err := Validate(request, request.UID); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Request){
		"wrong invoking uid": func(r *Request) { r.UID++ },
		"network listener":   func(r *Request) { r.HelperListenAddress = "0.0.0.0:8080" },
		"control downgrade":  func(r *Request) { r.ControlURL = "http://control.example.test" },
		"relative state":     func(r *Request) { r.StateRoot = "state" },
		"environment path":   func(r *Request) { r.Path += "\nLD_PRELOAD=/tmp/x" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if err := Validate(changed, request.UID); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if err := os.WriteFile(request.Executable, []byte("tampered"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := Validate(request, request.UID); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("tampered artifact error=%v", err)
	}
}

func TestValidateRejectsSymlinkedArtifact(t *testing.T) {
	request := validRequest(t)
	link := filepath.Join(t.TempDir(), "pbh")
	if err := os.Symlink(request.Executable, link); err != nil {
		t.Fatal(err)
	}
	request.Executable = link
	if err := Validate(request, request.UID); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("symlink error=%v", err)
	}
}

func validRequest(t *testing.T) Request {
	t.Helper()
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	uid, _ := strconv.Atoi(account.Uid)
	gid, _ := strconv.Atoi(account.Gid)
	group, err := user.LookupGroupId(account.Gid)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 32)
	if runtime.GOOS == "linux" {
		copy(body, "\x7fELF")
		body[4], body[5] = 2, 1
		machine := uint16(62)
		if runtime.GOARCH == "arm64" {
			machine = 183
		}
		binary.LittleEndian.PutUint16(body[18:20], machine)
	} else {
		binary.LittleEndian.PutUint32(body[:4], 0xfeedfacf)
		cpu := uint32(0x01000007)
		if runtime.GOARCH == "arm64" {
			cpu = 0x0100000c
		}
		binary.LittleEndian.PutUint32(body[4:8], cpu)
	}
	executable := filepath.Join(t.TempDir(), "pbh")
	if err := os.WriteFile(executable, body, 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	manifest := bootstrap.ArtifactManifest{Schema: bootstrap.ArtifactSchemaV2, Kind: bootstrap.ArtifactKindWorker, Version: "test", Platform: runtime.GOOS, Architecture: runtime.GOARCH, URL: "https://example.test/pbh", ByteLength: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(value *bootstrap.ArtifactManifest) {
		payload, _ := json.Marshal(struct {
			Architecture string `json:"architecture"`
			ByteLength   int64  `json:"byte_length"`
			Kind         string `json:"kind"`
			Platform     string `json:"platform"`
			Schema       string `json:"schema"`
			SHA256       string `json:"sha256"`
			URL          string `json:"url"`
			Version      string `json:"version"`
		}{value.Architecture, value.ByteLength, value.Kind, value.Platform, value.Schema, value.SHA256, value.URL, value.Version})
		value.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, payload))
	}
	sign(&manifest)
	hostManifest := manifest
	hostManifest.Kind = bootstrap.ArtifactKindHostService
	hostManifest.URL = "https://example.test/paperboat-host-service"
	sign(&hostManifest)
	state := t.TempDir()
	state, err = filepath.EvalSymlinks(state)
	if err != nil {
		t.Fatal(err)
	}
	herdr := filepath.Join(t.TempDir(), "herdr")
	if err := os.WriteFile(herdr, []byte("herdr"), 0o700); err != nil {
		t.Fatal(err)
	}
	return Request{
		Schema: SchemaV1, Platform: runtime.GOOS, User: account.Username, UID: uid, Group: group.Name, GID: gid,
		Executable: executable, Artifact: manifest, HostExecutable: executable, HostArtifact: hostManifest, ArtifactPublicKey: base64.RawURLEncoding.EncodeToString(public),
		Home: account.HomeDir, Path: "/usr/bin:/bin", StateRoot: state, WorkspaceRoot: account.HomeDir,
		ControlURL: "https://control.example.test", UserMachineID: "um_test", Shell: "/bin/sh",
		HelperListenAddress: "127.0.0.1:8080", HerdrPath: herdr, HerdrVersion: "test",
	}
}
