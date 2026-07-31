package configsync

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func manifestTestRuntimePolicy() RuntimePolicy {
	return RuntimePolicy{MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20, SummaryLimit: 100}
}

func TestParseManifestNormalizesRootsAndMatchesIgnores(t *testing.T) {
	manifest, err := ParseManifest(
		[]byte("# explicit roots\r\n.config/tool/\r\n.config/tool/settings.json\r\n.gitconfig\r\n.gitconfig\r\n"),
		[]byte("*.tmp\n**/cache/\n!**/cache/keep.txt\n"),
		DefaultManifestLimits(),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []ManifestRoot{{Path: ".config/tool", Directory: true}, {Path: ".gitconfig"}}
	if !reflect.DeepEqual(manifest.Roots, want) {
		t.Fatalf("roots = %#v, want %#v", manifest.Roots, want)
	}
	for _, test := range []struct {
		path string
		dir  bool
		want bool
	}{
		{path: ".config/tool/config.json", want: true},
		{path: ".config/tool/config.tmp", want: false},
		{path: ".config/tool/cache/value", want: false},
		{path: ".config/tool/cache/keep.txt", want: true},
		{path: ".gitconfig", want: true},
		{path: ".gitconfig/child", want: false},
		{path: ".config/other", want: false},
	} {
		if got := manifest.Manages(test.path, test.dir); got != test.want {
			t.Errorf("Manages(%q) = %v, want %v", test.path, got, test.want)
		}
	}
}

func TestParseManifestEmptyIsHealthy(t *testing.T) {
	manifest, err := ParseManifest(nil, nil, DefaultManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Roots) != 0 || manifest.Manages(".config/tool", false) {
		t.Fatalf("empty manifest selected paths: %#v", manifest.Roots)
	}
}

func TestParseManifestRejectsUnsafeAndInvalidInput(t *testing.T) {
	for _, value := range [][]byte{
		[]byte("../secret\n"), []byte("/etc/passwd\n"), []byte("C:/secret\n"),
		[]byte(".config/*\n"), []byte(".ssh/config\n"), []byte(".env\n"),
		[]byte(".config/tool/cache.sqlite\n"), []byte("bad\x00path\n"),
	} {
		_, err := ParseManifest(value, nil, DefaultManifestLimits())
		if !errors.Is(err, ErrManifestUnsafePath) && !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("ParseManifest(%q) error = %v", value, err)
		}
	}
	limits := DefaultManifestLimits()
	limits.MaxPatternBytes = 3
	if _, err := ParseManifest([]byte("long\n"), nil, limits); !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("oversized line error = %v", err)
	}
}

func TestLoadManifestRequiresRegularInclude(t *testing.T) {
	root := t.TempDir()
	if _, err := LoadManifest(root, DefaultManifestLimits()); !errors.Is(err, ErrManifestMissing) {
		t.Fatalf("missing include error = %v", err)
	}
	if err := os.Symlink("target", filepath.Join(root, ".pbinclude")); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(root, DefaultManifestLimits()); !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("symlink include error = %v", err)
	}
}

func TestManifestCannotNegateBeyondAllowlistOrHardExclusions(t *testing.T) {
	manifest, err := ParseManifest([]byte(".config/tool/\n"), []byte("*\n!**\n"), DefaultManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Manages(".config/other/value", false) || manifest.Manages(".ssh/config", false) {
		t.Fatal("ignore negation expanded the allowlist")
	}
	if !manifest.Manages(".config/tool/value", false) {
		t.Fatal("negation did not re-include an allowlisted path")
	}
}

func TestTakeManifestSnapshotSelectsRootsAndNewDescendants(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".config", "tool", "cache"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		".config/tool/config": "config", ".config/tool/cache/ignored": "ignored", ".outside": "outside",
	} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := ParseManifest([]byte(".config/tool/\n"), []byte("**/cache/\n"), DefaultManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	policy := manifestTestRuntimePolicy()
	snapshot, err := TakeManifestSnapshot(root, policy, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Files[".config/tool/config"]; !ok || len(snapshot.Files) != 1 {
		t.Fatalf("snapshot files = %#v", snapshot.Files)
	}
}

func TestTakeManifestSnapshotEmptyDoesNotTraverseHome(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "unreadable"), 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(root, "unreadable"), 0o700) })
	manifest, err := ParseManifest(nil, nil, DefaultManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := TakeManifestSnapshot(root, manifestTestRuntimePolicy(), manifest)
	if err != nil || len(snapshot.Files) != 0 {
		t.Fatalf("empty snapshot = %#v, %v", snapshot, err)
	}
}

func TestTakeManifestSnapshotRejectsSymlinkedAncestor(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	manifest, err := ParseManifest([]byte("linked/config\n"), nil, DefaultManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TakeManifestSnapshot(root, manifestTestRuntimePolicy(), manifest); !errors.Is(err, ErrManifestUnsafePath) {
		t.Fatalf("symlinked ancestor error = %v", err)
	}
}
