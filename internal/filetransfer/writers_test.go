package filetransfer

import (
	"errors"
	"testing"
	"time"
)

func TestWriterRegistrySelectsLastInputAndSoleFallbackWithoutSwitchingState(t *testing.T) {
	r := NewWriterRegistry()
	r.Attach("ses", "att_a", "cli_a")
	if got, err := r.Recipient("ses"); err != nil || got != "cli_a" {
		t.Fatalf("sole=%q err=%v", got, err)
	}
	r.Attach("ses", "att_b", "cli_b")
	if _, err := r.Recipient("ses"); !errors.Is(err, ErrNoActiveWriter) {
		t.Fatalf("ambiguous err=%v", err)
	}
	now := time.Now()
	r.Input("ses", "att_a", "cli_a", now)
	r.Input("ses", "att_b", "cli_b", now.Add(time.Second))
	if got, err := r.Recipient("ses"); err != nil || got != "cli_b" {
		t.Fatalf("last=%q err=%v", got, err)
	}
	r.Detach("ses", "att_b")
	if got, err := r.Recipient("ses"); err != nil || got != "cli_a" {
		t.Fatalf("fallback=%q err=%v", got, err)
	}
}
