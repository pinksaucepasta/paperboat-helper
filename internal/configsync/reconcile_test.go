package configsync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestPlanReconciliationModes(t *testing.T) {
	state := func(hash string) FileState { return FileState{Hash: hash} }
	base := map[string]FileState{"local": state("base"), "remote": state("base")}
	local := map[string]FileState{"local": state("local"), "remote": state("base")}
	remote := map[string]FileState{"local": state("base"), "remote": state("remote")}

	pull := PlanReconciliationMode(base, local, remote, ModePullOnly)
	if len(pull.PublishUpdates) != 0 || len(pull.ApplyRemote) != 1 || pull.ApplyRemote[0] != "remote" || len(pull.Conflicts) != 0 {
		t.Fatalf("pull-only plan = %#v", pull)
	}
	push := PlanReconciliationMode(base, local, remote, ModePushOnly)
	if len(push.PublishUpdates) != 1 || push.PublishUpdates[0] != "local" || len(push.ApplyRemote) != 0 ||
		len(push.Conflicts) != 1 || push.Conflicts[0].Path != "remote" {
		t.Fatalf("push-only plan = %#v", push)
	}
	bidirectional := PlanReconciliationMode(base, local, remote, ModeBidirectional)
	if len(bidirectional.PublishUpdates) != 1 || len(bidirectional.ApplyRemote) != 1 || len(bidirectional.Conflicts) != 0 {
		t.Fatalf("bidirectional plan = %#v", bidirectional)
	}
}

func TestPullOnlyConflictsWhenIncomingChangeTouchesLocalDivergence(t *testing.T) {
	base := map[string]FileState{"path": {Hash: "base"}}
	local := map[string]FileState{"path": {Hash: "local"}}
	remote := map[string]FileState{"path": {Hash: "remote"}}
	plan := PlanReconciliationMode(base, local, remote, ModePullOnly)
	if len(plan.Conflicts) != 1 || plan.Conflicts[0].Path != "path" {
		t.Fatalf("pull-only conflict plan = %#v", plan)
	}
}

func TestAcceptedBaselineRetainsUnpublishedDivergence(t *testing.T) {
	base := map[string]FileState{"local": {Hash: "base"}, "remote": {Hash: "base"}}
	local := map[string]FileState{"local": {Hash: "local"}, "remote": {Hash: "remote"}}
	remote := map[string]FileState{"local": {Hash: "base"}, "remote": {Hash: "remote"}}
	accepted := AcceptedBaseline(base, local, remote, ModePullOnly, true)
	if accepted["local"].Hash != "base" || accepted["remote"].Hash != "remote" {
		t.Fatalf("pull-only accepted baseline = %#v", accepted)
	}

	accepted = AcceptedBaseline(base, local, remote, ModeBidirectional, false)
	if accepted["local"].Hash != "base" || accepted["remote"].Hash != "remote" {
		t.Fatalf("read-only rollout accepted baseline = %#v", accepted)
	}
}

func TestAcceptedBaselineUsesPublishedLocalState(t *testing.T) {
	local := map[string]FileState{"path": {Hash: "local"}}
	accepted := AcceptedBaseline(map[string]FileState{}, local, map[string]FileState{}, ModePushOnly, true)
	local["path"] = FileState{Hash: "changed-after-copy"}
	if accepted["path"].Hash != "local" {
		t.Fatalf("published baseline = %#v", accepted)
	}
}

func TestFreezeConflictBaselineRetainsAncestry(t *testing.T) {
	previous := map[string]FileState{"existing": {Hash: "base"}}
	accepted := map[string]FileState{
		"existing": {Hash: "local"},
		"created":  {Hash: "local-create"},
		"clean":    {Hash: "merged"},
	}
	freezeConflictBaseline(accepted, previous, []PathSummary{
		{Path: "existing"},
		{Path: "created"},
	})
	if accepted["existing"].Hash != "base" {
		t.Fatalf("existing ancestry = %#v", accepted["existing"])
	}
	if _, ok := accepted["created"]; ok {
		t.Fatalf("created conflict advanced baseline: %#v", accepted["created"])
	}
	if accepted["clean"].Hash != "merged" {
		t.Fatalf("clean path did not advance: %#v", accepted["clean"])
	}
}

func TestBaselineRoundTripUsesPrivateFile(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "state", "baseline.json")
	baseline := Baseline{
		Format: "paperboat-config-baseline-v1", RepositoryID: "repository",
		AssignmentID: "assignment", PolicyRevision: "policy",
		ManifestRevision: strings.Repeat("b", 64), SelectedRoots: []ManifestRoot{},
		FrozenPaths:    map[string]FrozenPath{},
		RemoteRevision: "revision", Files: map[string]FileState{
			".zshrc": {Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Bytes: 1, Mode: 0o600},
		},
	}
	if err := WriteBaseline(path, baseline); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBaseline(path)
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
	if !strings.Contains(string(data), baseline.Files[".zshrc"].Hash) {
		t.Fatal("baseline is not ordinary plaintext JSON")
	}
}
