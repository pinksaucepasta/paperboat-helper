//go:build unix

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSecureOpenNoFollowRejectsSymlinkAtEveryComponent(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "real")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(directory, "file.txt")
	if err := os.WriteFile(filePath, []byte("exact"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := secureOpenNoFollow(root, filePath)
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	finalLink := filepath.Join(directory, "final-link")
	if err := os.Symlink(filePath, finalLink); err != nil {
		t.Fatal(err)
	}
	if file, err := secureOpenNoFollow(root, finalLink); err == nil {
		_ = file.Close()
		t.Fatal("accepted final symlink")
	}
	directoryLink := filepath.Join(root, "directory-link")
	if err := os.Symlink(directory, directoryLink); err != nil {
		t.Fatal(err)
	}
	if file, err := secureOpenNoFollow(root, filepath.Join(directoryLink, "file.txt")); err == nil {
		_ = file.Close()
		t.Fatal("accepted intermediate symlink")
	}
}

func TestSendOperationIDsAreUniqueAndOpaque(t *testing.T) {
	first, err := newSendOperationID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newSendOperationID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || len(first) != len("send_")+32 || first[:5] != "send_" {
		t.Fatalf("operation IDs %q and %q", first, second)
	}
}
