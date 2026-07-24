package configsync

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"filippo.io/age"
)

func TestPlanReconciliationPreservesConcurrentChanges(t *testing.T) {
	state := func(hash string) FileState { return FileState{Hash: hash} }
	baseline := map[string]FileState{
		".conflict": state("base"), ".local": state("base"), ".remote": state("base"),
		".remote-delete": state("base"), ".local-delete": state("base"),
	}
	local := map[string]FileState{
		".conflict": state("local"), ".local": state("local"), ".remote": state("base"),
		".remote-delete": state("base"),
	}
	remote := map[string]FileState{
		".conflict": state("remote"), ".local": state("base"), ".remote": state("remote"),
		".local-delete": state("base"),
	}
	plan := PlanReconciliation(baseline, local, remote)
	if len(plan.Conflicts) != 1 || plan.Conflicts[0].Path != ".conflict" {
		t.Fatalf("conflicts = %#v", plan.Conflicts)
	}
	if len(plan.PublishUpdates) != 1 || plan.PublishUpdates[0] != ".local" ||
		len(plan.PublishDeletes) != 1 || plan.PublishDeletes[0] != ".local-delete" ||
		len(plan.ApplyRemote) != 1 || plan.ApplyRemote[0] != ".remote" ||
		len(plan.DeleteLocal) != 1 || plan.DeleteLocal[0] != ".remote-delete" {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestBaselineRoundTripUsesPrivateFile(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "state", "baseline.json")
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	baseline := Baseline{
		Format: "paperboat-config-baseline-v1", RepositoryID: "repository",
		AssignmentID: "assignment", PolicyRevision: "policy", KeyVersion: 1,
		RemoteRevision: "revision", Files: map[string]FileState{
			".zshrc": {Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Bytes: 1, Mode: 0o600},
		},
	}
	if err := WriteBaseline(path, baseline, identity.Recipient().String()); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBaseline(path, identity.String())
	if err != nil || got.Files[".zshrc"] != baseline.Files[".zshrc"] {
		t.Fatalf("baseline = %#v, %v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, %v", info.Mode(), err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(baseline.Files[".zshrc"].Hash)) {
		t.Fatal("baseline content hash was stored in plaintext")
	}
}
