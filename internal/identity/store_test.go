package identity

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestStoreCreatesPrivateStableSigningIdentityAndRotatesAtomically(t *testing.T) {
	root := filepath.Join(t.TempDir(), "identity")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	random := append(bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)...)
	store, err := Open(Config{StateRoot: root, Random: bytes.NewReader(random), Clock: fixedClock{now}})
	if err != nil {
		t.Fatal(err)
	}
	first := store.Current()
	if first.ID == "" || first.Thumbprint == "" || first.CreatedAt != now {
		t.Fatal("invalid initial key metadata")
	}
	message := []byte("helper identity proof")
	if !ed25519.Verify(first.Public(), message, first.Sign(message)) {
		t.Fatal("signature failed")
	}
	info, err := os.Stat(filepath.Join(root, "helper-identity.json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("info=%v err=%v", info, err)
	}
	reopened, err := Open(Config{StateRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Current().ID != first.ID {
		t.Fatal("identity changed on reopen")
	}
	second, err := store.Rotate(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID {
		t.Fatal("rotation retained key")
	}
	reopened, err = Open(Config{StateRoot: root})
	if err != nil || reopened.Current().ID != second.ID {
		t.Fatalf("rotated identity did not persist: %v", err)
	}
	if _, err := store.Rotate(first.ID); !errors.Is(err, ErrKeyConflict) {
		t.Fatalf("stale rotate err=%v", err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestFailedRotationPreservesCurrentIdentity(t *testing.T) {
	root := filepath.Join(t.TempDir(), "identity")
	store, err := Open(Config{StateRoot: root, Random: bytes.NewReader(bytes.Repeat([]byte{1}, 32))})
	if err != nil {
		t.Fatal(err)
	}
	first := store.Current()
	store.config.Random = failingReader{}
	if _, err := store.Rotate(first.ID); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err=%v", err)
	}
	reopened, err := Open(Config{StateRoot: root})
	if err != nil || reopened.Current().ID != first.ID {
		t.Fatalf("failed rotation changed identity: %v", err)
	}
}

func TestStoreRejectsSymlinkHardlinkAndDuplicateJSON(t *testing.T) {
	base := t.TempDir()
	realRoot := filepath.Join(base, "real")
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(base, "link")
	if err := os.Symlink(realRoot, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{StateRoot: symlink}); !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("symlink err=%v", err)
	}
	store, err := Open(Config{StateRoot: realRoot, Random: bytes.NewReader(bytes.Repeat([]byte{2}, 64))})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(realRoot, "helper-identity.json")
	link := filepath.Join(realRoot, "copied-secret")
	if err := os.Link(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Rotate(store.Current().ID); !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("hardlink rotate err=%v", err)
	}
	if _, err := Open(Config{StateRoot: realRoot}); !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("hardlink open err=%v", err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"version":1,"key_id":"x","seed_base64url":"x","created_at":"2026-01-01T00:00:00Z"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{StateRoot: realRoot}); !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("duplicate err=%v", err)
	}
}
