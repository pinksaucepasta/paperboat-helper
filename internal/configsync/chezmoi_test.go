package configsync

import (
	"os"
	"path/filepath"
	"testing"

	"filippo.io/age"
)

func TestEncryptedRepositoryFormatAndUnsafeSourceValidation(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	format := EncryptedRepositoryFormat{
		Format: "paperboat-chezmoi-age", Version: 1, KeyVersion: 1,
		Recipient: identity.Recipient().String(),
	}
	if err := WriteEncryptedRepositoryFormat(root, format); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadEncryptedRepositoryFormat(root); err != nil || got != format {
		t.Fatalf("format = %#v, %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(root, "run_once_bad"), []byte("encrypted"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateEncryptedRepository(root); err == nil {
		t.Fatal("executable chezmoi source accepted")
	}
}

func TestWriteAgeIdentityUsesPrivateCreateOnceFile(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "private", "identity.txt")
	if err := WriteAgeIdentity(path, "AGE-SECRET-KEY-TEST"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("identity mode = %v, %v", info.Mode(), err)
	}
	if err := WriteAgeIdentity(path, "replacement"); err == nil {
		t.Fatal("identity overwrite accepted")
	}
	if err := EnsureAgeIdentity(path, "replacement"); err != nil {
		t.Fatal(err)
	}
	value, err := os.ReadFile(path)
	if err != nil || string(value) != "replacement\n" {
		t.Fatalf("identity = %q, %v", value, err)
	}
}
