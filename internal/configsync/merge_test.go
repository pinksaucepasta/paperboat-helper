package configsync

import (
	"context"
	"errors"
	"os/exec"
	"testing"
)

func TestMergeRegularText(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is unavailable")
	}
	tests := []struct {
		name   string
		base   string
		local  string
		remote string
		want   string
		clean  bool
	}{
		{name: "non-overlapping", base: "one\ntwo\nthree\n", local: "ONE\ntwo\nthree\n", remote: "one\ntwo\nTHREE\n", want: "ONE\ntwo\nTHREE\n", clean: true},
		{name: "overlapping", base: "one\n", local: "local\n", remote: "remote\n"},
		{name: "binary", base: "one\x00", local: "local\x00", remote: "remote\x00"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, mergeErr := mergeRegularText(context.Background(), git, []byte(test.base), []byte(test.local), []byte(test.remote), 1<<20)
			if test.clean {
				if mergeErr != nil || string(got) != test.want {
					t.Fatalf("merge = %q, %v", got, mergeErr)
				}
				return
			}
			if !errors.Is(mergeErr, ErrTextMergeConflict) {
				t.Fatalf("error = %v", mergeErr)
			}
		})
	}
}

func TestMergeRegularTextHonorsCancellationAndOutputBound(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is unavailable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mergeRegularText(ctx, git, []byte("a\n"), []byte("b\n"), []byte("a\n"), 1<<20); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	if _, err := mergeRegularText(context.Background(), git, []byte("a\n"), []byte("long output\n"), []byte("a\n"), 2); !errors.Is(err, ErrTextMergeConflict) {
		t.Fatalf("bound error = %v", err)
	}
}
