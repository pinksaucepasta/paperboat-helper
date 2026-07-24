package configsync

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTakeSnapshotIsBoundedAndRejectsUnsafeEntries(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".zshrc"), []byte("portable"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".large"), []byte("too large"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".ssh", "id"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".config", "paperboat", "helper"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".config", "paperboat", "helper", "agent-token"), []byte("runtime secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../outside", filepath.Join(root, ".escape")); err != nil {
		t.Fatal(err)
	}
	policy := RuntimePolicy{
		MaxFileBytes: 8, MaxBatchBytes: 16,
		Includes:            []string{".zshrc", ".large", ".escape", ".ssh/**", ".config/paperboat/**"},
		MandatoryExclusions: append([]string(nil), requiredMandatoryExclusions...),
	}
	snapshot, err := TakeSnapshot(root, policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Files[".zshrc"]; !ok {
		t.Fatalf("portable file absent: %#v", snapshot)
	}
	if _, ok := snapshot.Files[".ssh/id"]; ok {
		t.Fatal("mandatory-excluded secret was captured")
	}
	if _, ok := snapshot.Files[".config/paperboat/helper/agent-token"]; ok {
		t.Fatal("Paperboat runtime credential was captured")
	}
	reasons := make(map[string]string)
	for _, skipped := range snapshot.Skipped {
		reasons[skipped.Path] = skipped.Reason
	}
	if reasons[".large"] != "max_file_bytes" || reasons[".escape"] != "unsafe_symlink" {
		t.Fatalf("skipped = %#v", snapshot.Skipped)
	}
}

func TestTakeSnapshotWithEmptyIncludesIsFailClosed(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, ".cache"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".cache", "unmanaged"), []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot, err := TakeSnapshot(root, RuntimePolicy{MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Files) != 0 || len(snapshot.Skipped) != 0 {
		t.Fatalf("empty includes observed filesystem entries: %#v", snapshot)
	}
	if managedPath(".cache/unmanaged", RuntimePolicy{}) || mayContainManagedPath(".cache", RuntimePolicy{}) {
		t.Fatal("empty includes retained implicit hidden-tree scope")
	}
}

func TestTakeSnapshotTraversesLeadingGlobIncludes(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".config", "editor"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".config", "editor", ".vimrc"), []byte("portable"), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot, err := TakeSnapshot(root, RuntimePolicy{
		Includes: []string{"**/.vimrc"}, MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Files[".config/editor/.vimrc"]; !ok {
		t.Fatalf("leading glob did not traverse matching subtree: %#v", snapshot.Files)
	}
}

func TestChangedPathsIncludesAddModifyAndDelete(t *testing.T) {
	before := map[string]FileState{
		".deleted": {Hash: "old"},
		".same":    {Hash: "same"},
		".changed": {Hash: "old"},
	}
	after := map[string]FileState{
		".same":    {Hash: "same"},
		".changed": {Hash: "new"},
		".added":   {Hash: "new"},
	}
	got := ChangedPaths(before, after)
	want := []string{".added", ".changed", ".deleted"}
	if len(got) != len(want) {
		t.Fatalf("changed = %#v", got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("changed = %#v", got)
		}
	}
}
