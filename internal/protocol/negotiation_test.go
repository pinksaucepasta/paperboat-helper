package protocol

import (
	"errors"
	"slices"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/config"
)

func TestBYODNegotiationFiltersHostedAndUnprovedConfig(t *testing.T) {
	available := map[string]bool{"terminal.v1": true, "health.v1": true, "config.apply.v1": true, "hosted.lifecycle.v1": true}
	w, err := (Negotiator{Profile: config.BYOD, Available: available}).Negotiate("1.0", "1.0", []string{"terminal.v1", "health.v1", "config.apply.v1", "hosted.lifecycle.v1", "future.v1"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(w.Capabilities, []string{"health.v1", "terminal.v1"}) {
		t.Fatalf("capabilities=%v", w.Capabilities)
	}
}

func TestNegotiationRequiresVersionAndRequiredCapabilities(t *testing.T) {
	n := Negotiator{Profile: config.Hosted, Available: map[string]bool{"terminal.v1": true, "health.v1": true}}
	for _, tc := range []struct {
		min, max string
		offered  []string
		code     Code
	}{{"0.1", "0.2", []string{"terminal.v1", "health.v1"}, ProtocolIncompatible}, {"1.0", "1.0", []string{"health.v1"}, CapabilityRequired}} {
		_, err := n.Negotiate(tc.min, tc.max, tc.offered)
		var pe *Error
		if !errors.As(err, &pe) || pe.Code != tc.code {
			t.Fatalf("err=%v want=%s", err, tc.code)
		}
	}
	if _, err := n.Negotiate("1.0", "1.1", []string{"terminal.v1", "health.v1"}); err != nil {
		t.Fatalf("compatible minor range: %v", err)
	}
}

type capabilityProvider []string

func (p capabilityProvider) Capabilities() []string { return append([]string(nil), p...) }

func TestAvailableCapabilitiesDeriveFromImplementedProviders(t *testing.T) {
	available, err := AvailableCapabilities(capabilityProvider{"terminal.v1", "health.v1"}, capabilityProvider{"preview.public.v1"})
	if err != nil || !available["preview.public.v1"] || available["config.apply.v1"] {
		t.Fatalf("available=%v err=%v", available, err)
	}
	for _, providers := range [][]CapabilityProvider{{capabilityProvider{"terminal.v1"}}, {capabilityProvider{"terminal.v1", "health.v1"}, capabilityProvider{"health.v1"}}, {nil}} {
		if _, err := AvailableCapabilities(providers...); !errors.Is(err, ErrInvalidCapabilities) {
			t.Fatalf("providers=%v err=%v", providers, err)
		}
	}
}
