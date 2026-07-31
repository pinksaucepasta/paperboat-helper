package configsync

import "testing"

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
			t.Fatalf("changed = %#v, want %#v", got, want)
		}
	}
}
