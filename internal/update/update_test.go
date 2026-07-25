package update

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type fetcher struct {
	body []byte
	err  error
}

func (f fetcher) Fetch(context.Context, string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.body)), nil
}

type checker struct {
	failCurrent bool
	calls       []string
}

func (c *checker) Check(_ context.Context, path, _ string) error {
	c.calls = append(c.calls, path)
	if c.failCurrent && filepath.Base(path) == "pbh" {
		return errors.New("unhealthy")
	}
	return nil
}

func setup(t *testing.T, artifact []byte, health *checker) (Config, ed25519.PrivateKey, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	install := filepath.Join(root, "bin", "pbh")
	if err := os.MkdirAll(filepath.Dir(install), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(install, []byte("old"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{StateRoot: filepath.Join(root, "state"), InstallPath: install, CurrentVersion: "1.0.0", Channel: "stable", ProtocolVersion: "1.0", StoreVersion: 1, TrustedKeys: map[string]ed25519.PublicKey{"root-1": public}, AllowedHosts: map[string]bool{"updates.example.test": true}, Fetcher: fetcher{body: artifact}, Health: health, MaxArtifactBytes: 1 << 20}
	return config, private, install
}

func signedEnvelope(t *testing.T, key ed25519.PrivateKey, artifact []byte, mutate func(*Manifest)) []byte {
	return signedEnvelopeWithKey(t, "root-1", key, artifact, mutate)
}

func signedEnvelopeWithKey(t *testing.T, keyID string, key ed25519.PrivateKey, artifact []byte, mutate func(*Manifest)) []byte {
	t.Helper()
	digest := sha256.Sum256(artifact)
	manifest := Manifest{Version: "1.1.0", Channel: "stable", PublishedAt: time.Now().UTC(), MinProtocol: "1.0", MaxProtocol: "1.1", MinStore: 1, MaxStore: 1, Artifacts: map[string]Artifact{runtime.GOOS + "-" + runtime.GOARCH: {URL: "https://updates.example.test/helper", SHA256: hex.EncodeToString(digest[:]), Size: int64(len(artifact))}}}
	if mutate != nil {
		mutate(&manifest)
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	envelope := Envelope{KeyID: keyID, Manifest: base64.RawURLEncoding.EncodeToString(manifestBytes), Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, manifestBytes))}
	result, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func signedTrustEnvelope(t *testing.T, signerID string, signer ed25519.PrivateKey, bundle TrustBundle) []byte {
	t.Helper()
	bundleBytes, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	envelope := TrustEnvelope{SignerKeyID: signerID, Bundle: base64.RawURLEncoding.EncodeToString(bundleBytes), Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(signer, bundleBytes))}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestSignedUpdateActivatesAndPreservesPrevious(t *testing.T) {
	artifact := []byte("new verified binary")
	health := &checker{}
	config, key, install := setup(t, artifact, health)
	manager, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := manager.Apply(context.Background(), signedEnvelope(t, key, artifact, nil))
	if err != nil || manifest.Version != "1.1.0" {
		t.Fatalf("manifest=%#v err=%v", manifest, err)
	}
	current, err := os.ReadFile(install)
	if err != nil || !bytes.Equal(current, artifact) {
		t.Fatalf("current=%q err=%v", current, err)
	}
	previous, err := os.ReadFile(install + ".previous")
	if err != nil || string(previous) != "old" {
		t.Fatalf("previous=%q err=%v", previous, err)
	}
	if len(health.calls) != 2 {
		t.Fatalf("health calls=%v", health.calls)
	}
}

func TestBadSignatureDigestAndCompatibilityFailBeforeActivation(t *testing.T) {
	artifact := []byte("new")
	config, key, install := setup(t, artifact, &checker{})
	manager, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	badSignature := signedEnvelope(t, key, artifact, nil)
	badSignature[len(badSignature)-2] ^= 1
	if _, err := manager.Apply(context.Background(), badSignature); !errors.Is(err, ErrSignatureInvalid) && !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("signature err=%v", err)
	}
	badDigest := signedEnvelope(t, key, artifact, func(manifest *Manifest) {
		item := manifest.Artifacts[runtime.GOOS+"-"+runtime.GOARCH]
		item.SHA256 = string(bytes.Repeat([]byte{'0'}, 64))
		manifest.Artifacts[runtime.GOOS+"-"+runtime.GOARCH] = item
	})
	if _, err := manager.Apply(context.Background(), badDigest); !errors.Is(err, ErrArtifactInvalid) {
		t.Fatalf("digest err=%v", err)
	}
	incompatible := signedEnvelope(t, key, artifact, func(manifest *Manifest) { manifest.MinStore = 2 })
	if _, err := manager.Apply(context.Background(), incompatible); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("compat err=%v", err)
	}
	current, _ := os.ReadFile(install)
	if string(current) != "old" {
		t.Fatalf("current=%q", current)
	}
}

func TestFailedPostActivationHealthRollsBack(t *testing.T) {
	artifact := []byte("new")
	health := &checker{failCurrent: true}
	config, key, install := setup(t, artifact, health)
	manager, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Apply(context.Background(), signedEnvelope(t, key, artifact, nil)); !errors.Is(err, ErrHealthCheck) {
		t.Fatalf("err=%v", err)
	}
	current, err := os.ReadFile(install)
	if err != nil || string(current) != "old" {
		t.Fatalf("current=%q err=%v", current, err)
	}
}

func TestRecoveryDistinguishesPreBackupAndPostBackup(t *testing.T) {
	artifact := []byte("new")
	config, _, install := setup(t, artifact, &checker{})
	manager, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(filepath.Dir(install), ".pbh-update-test")
	if err := os.WriteFile(staged, artifact, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := manager.writeJournal(journal{State: "backing_up", Version: "1.1.0", PreviousVersion: "1.0.0", StagedPath: staged}); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	current, _ := os.ReadFile(install)
	if string(current) != "old" {
		t.Fatalf("pre-backup current=%q", current)
	}
	if err := os.WriteFile(staged, artifact, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(install, install+".previous"); err != nil {
		t.Fatal(err)
	}
	manager, err = New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.writeJournal(journal{State: "activating", Version: "1.1.0", PreviousVersion: "1.0.0", StagedPath: staged}); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	current, _ = os.ReadFile(install)
	if string(current) != "old" {
		t.Fatalf("post-backup current=%q", current)
	}
}

func TestRecoveryMatrixPreservesOrRestoresVerifiedArtifact(t *testing.T) {
	type recoveryCase struct {
		state       string
		backup      bool
		activate    bool
		unhealthy   bool
		want        string
		journalKept bool
	}
	for _, test := range []recoveryCase{
		{state: "staged", want: "old"},
		{state: "backing_up", want: "old"},
		{state: "backing_up", backup: true, want: "old"},
		{state: "activating", backup: true, want: "old"},
		{state: "activating", backup: true, activate: true, want: "old"},
		{state: "checking", backup: true, activate: true, want: "old"},
		{state: "committed", backup: true, activate: true, want: "new", journalKept: true},
		{state: "committed", backup: true, activate: true, unhealthy: true, want: "old"},
	} {
		name := fmt.Sprintf("%s_backup_%v_activate_%v_unhealthy_%v", test.state, test.backup, test.activate, test.unhealthy)
		t.Run(name, func(t *testing.T) {
			artifact := []byte("new")
			health := &checker{failCurrent: test.unhealthy}
			config, _, install := setup(t, artifact, health)
			manager, err := New(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			staged := filepath.Join(filepath.Dir(install), ".pbh-update-recovery")
			if err := os.WriteFile(staged, artifact, 0o700); err != nil {
				t.Fatal(err)
			}
			if test.backup {
				if err := os.Rename(install, install+".previous"); err != nil {
					t.Fatal(err)
				}
			}
			if test.activate {
				if err := os.Rename(staged, install); err != nil {
					t.Fatal(err)
				}
			}
			stagedPath := staged
			if test.state == "checking" || test.state == "committed" {
				stagedPath = ""
			}
			if err := manager.writeJournal(journal{State: test.state, Version: "1.1.0", PreviousVersion: "1.0.0", StagedPath: stagedPath}); err != nil {
				t.Fatal(err)
			}
			if _, err := New(context.Background(), config); err != nil {
				t.Fatal(err)
			}
			current, err := os.ReadFile(install)
			if err != nil || string(current) != test.want {
				t.Fatalf("current=%q err=%v", current, err)
			}
			_, journalErr := os.Stat(filepath.Join(config.StateRoot, "update-journal.json"))
			if test.journalKept && journalErr != nil || !test.journalKept && !errors.Is(journalErr, os.ErrNotExist) {
				t.Fatalf("journal kept=%v err=%v", test.journalKept, journalErr)
			}
			if test.state != "committed" {
				if _, err := os.Stat(staged); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("staged err=%v", err)
				}
			}
		})
	}
}

func TestRecoveryRejectsInvalidJournalWithoutDeletingUnexplainedState(t *testing.T) {
	for _, entry := range []journal{
		{State: "staged", Version: "invalid", PreviousVersion: "1.0.0", StagedPath: "/tmp/.pbh-update-outside"},
		{State: "staged", Version: "1.1.0", PreviousVersion: "0.9.0", StagedPath: "/tmp/.pbh-update-outside"},
		{State: "checking", Version: "1.1.0", PreviousVersion: "1.0.0", StagedPath: "/tmp/.pbh-update-outside"},
	} {
		t.Run(entry.State+"_"+entry.Version+"_"+entry.PreviousVersion, func(t *testing.T) {
			config, _, install := setup(t, []byte("new"), &checker{})
			manager, err := New(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.writeJournal(entry); err != nil {
				t.Fatal(err)
			}
			if _, err := New(context.Background(), config); !errors.Is(err, ErrManifestInvalid) {
				t.Fatalf("err=%v", err)
			}
			current, err := os.ReadFile(install)
			if err != nil || string(current) != "old" {
				t.Fatalf("current=%q err=%v", current, err)
			}
			if _, err := os.Stat(filepath.Join(config.StateRoot, "update-journal.json")); err != nil {
				t.Fatalf("journal was not preserved: %v", err)
			}
		})
	}
}

func TestTrustRotationPersistsAndRevokesKeysAndVersions(t *testing.T) {
	artifact := []byte("new")
	now := time.Now().UTC()
	config, root1, _ := setup(t, artifact, &checker{})
	config.Clock = func() time.Time { return now }
	manager, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	public2, root2, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	public1 := root1.Public().(ed25519.PublicKey)
	first := TrustBundle{Generation: 1, IssuedAt: now, Keys: map[string]string{"root-1": base64.RawURLEncoding.EncodeToString(public1), "root-2": base64.RawURLEncoding.EncodeToString(public2)}, RevokedKeyIDs: []string{}, RevokedVersions: []string{}}
	if err := manager.ApplyTrustBundle(signedTrustEnvelope(t, "root-1", root1, first)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.verify(signedEnvelopeWithKey(t, "root-2", root2, artifact, func(manifest *Manifest) { manifest.Version = "1.2.0" })); err != nil {
		t.Fatalf("rotated key: %v", err)
	}
	second := TrustBundle{Generation: 2, IssuedAt: now, Keys: map[string]string{"root-2": base64.RawURLEncoding.EncodeToString(public2)}, RevokedKeyIDs: []string{"root-1"}, RevokedVersions: []string{"1.2.0"}}
	if err := manager.ApplyTrustBundle(signedTrustEnvelope(t, "root-1", root1, second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.verify(signedEnvelopeWithKey(t, "root-2", root2, artifact, func(manifest *Manifest) { manifest.Version = "1.2.0" })); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("revoked version err=%v", err)
	}
	if _, _, err := manager.verify(signedEnvelopeWithKey(t, "root-1", root1, artifact, func(manifest *Manifest) { manifest.Version = "1.3.0" })); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("revoked key err=%v", err)
	}
	restarted, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := restarted.verify(signedEnvelopeWithKey(t, "root-1", root1, artifact, func(manifest *Manifest) { manifest.Version = "1.3.0" })); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("persisted revocation err=%v", err)
	}
	if err := restarted.ApplyTrustBundle(signedTrustEnvelope(t, "root-2", root2, first)); !errors.Is(err, ErrTrustInvalid) {
		t.Fatalf("rollback trust err=%v", err)
	}
}

func TestTrustBundleDistributionIsHTTPSBoundedAndVerified(t *testing.T) {
	artifact := []byte("new")
	now := time.Now().UTC()
	config, root1, _ := setup(t, artifact, &checker{})
	config.Clock = func() time.Time { return now }
	manager, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	public2, _, _ := ed25519.GenerateKey(rand.Reader)
	bundle := TrustBundle{Generation: 1, IssuedAt: now, Keys: map[string]string{"root-2": base64.RawURLEncoding.EncodeToString(public2)}, RevokedKeyIDs: []string{"root-1"}, RevokedVersions: []string{}}
	envelope := signedTrustEnvelope(t, "root-1", root1, bundle)
	manager.config.Fetcher = fetcher{body: envelope}
	if err := manager.FetchAndApplyTrustBundle(context.Background(), "https://updates.example.test/trust"); err != nil {
		t.Fatal(err)
	}
	if err := manager.FetchAndApplyTrustBundle(context.Background(), "http://updates.example.test/trust"); !errors.Is(err, ErrTrustInvalid) {
		t.Fatalf("http err=%v", err)
	}
	manager.config.Fetcher = fetcher{body: bytes.Repeat([]byte("x"), (64<<10)+1)}
	if err := manager.FetchAndApplyTrustBundle(context.Background(), "https://updates.example.test/trust"); !errors.Is(err, ErrTrustInvalid) {
		t.Fatalf("oversize err=%v", err)
	}
}
