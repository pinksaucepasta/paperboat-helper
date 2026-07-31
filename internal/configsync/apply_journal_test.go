package configsync

import (
	"os"
	"path/filepath"
	"testing"
)

func TestApplyJournalRestoresOriginalsAndRemovesCreatedPaths(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "home")
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(filepath.Join(home, ".config"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(home, ".config", "original")
	if err := os.WriteFile(original, []byte("before\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(state, "apply-journal.json")
	paths := []string{".config/original", ".config/created"}
	if err := beginApplyJournal(
		journal, home, "repo", "assignment", "revision",
		paths, 1<<20,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(original, []byte("after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	created := filepath.Join(home, ".config", "created")
	if err := os.WriteFile(created, []byte("created\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := recoverApplyJournal(
		journal, home, "repo", "assignment", 1<<20,
	); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(original)
	if err != nil || string(content) != "before\n" {
		t.Fatalf("original = %q, %v", content, err)
	}
	info, err := os.Stat(original)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("original mode = %v, %v", info.Mode().Perm(), err)
	}
	if _, err := os.Lstat(created); !os.IsNotExist(err) {
		t.Fatalf("created path remains: %v", err)
	}
	if _, err := os.Lstat(journal); !os.IsNotExist(err) {
		t.Fatalf("journal remains: %v", err)
	}
}

func TestApplyJournalRejectsWrongAssignmentWithoutMutation(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "home")
	state := filepath.Join(root, "state")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(home, "value")
	if err := os.WriteFile(target, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(state, "apply-journal.json")
	if err := beginApplyJournal(
		journal, home, "repo", "assignment", "revision",
		[]string{"value"}, 1<<20,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := recoverApplyJournal(journal, home, "repo", "other", 1<<20); err == nil {
		t.Fatal("wrong assignment journal was accepted")
	}
	content, err := os.ReadFile(target)
	if err != nil || string(content) != "after" {
		t.Fatalf("target mutated = %q, %v", content, err)
	}
}
