package contracttest

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

var (
	errProtocol     = errors.New("protocol_incompatible")
	errCapability   = errors.New("capability_required")
	errReplayGap    = errors.New("replay_gap")
	errInvalidPath  = errors.New("invalid_path")
	errUnauthorized = errors.New("not_found_or_forbidden")
	errCanceled     = errors.New("operation_canceled")
	errSlowConsumer = errors.New("slow_consumer")
)

type fakePeer struct {
	version          string
	capabilities     map[string]bool
	earliestSequence uint64
	latestSequence   uint64
	revoked          bool
	canceled         map[string]bool
	queueBytes       int
}

func newFakePeer() *fakePeer {
	return &fakePeer{
		version: "1.0",
		capabilities: map[string]bool{
			"terminal.v1": true, "preview.public.v1": true,
			"config.apply.v1": true, "health.v1": true,
		},
		earliestSequence: 1024,
		latestSequence:   2048,
		canceled:         map[string]bool{},
	}
}

func (p *fakePeer) negotiate(versions, required []string) error {
	if p.revoked {
		return errUnauthorized
	}
	selected := false
	for _, version := range versions {
		selected = selected || version == p.version
	}
	if !selected {
		return errProtocol
	}
	for _, capability := range required {
		if !p.capabilities[capability] {
			return errCapability
		}
	}
	return nil
}

func (p *fakePeer) attach(from uint64) (uint64, uint64, error) {
	if p.revoked {
		return 0, 0, errUnauthorized
	}
	if from < p.earliestSequence {
		return p.earliestSequence, p.latestSequence, errReplayGap
	}
	return from, p.latestSequence, nil
}

func (p *fakePeer) stage(name string) error {
	if p.revoked {
		return errUnauthorized
	}
	clean := filepath.Clean(name)
	if filepath.IsAbs(name) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean != filepath.Base(clean) {
		return errInvalidPath
	}
	return nil
}

func (p *fakePeer) preview(private bool) error {
	if p.revoked || private {
		return errUnauthorized
	}
	return nil
}

func (p *fakePeer) config(hasScope, hasAssignment, hasConsent bool) error {
	if p.revoked || !hasScope || !hasAssignment || !hasConsent {
		return errUnauthorized
	}
	return nil
}

func (p *fakePeer) cancel(operationID string) error {
	p.canceled[operationID] = true
	return errCanceled
}

func (p *fakePeer) enqueue(bytes int) error {
	if p.queueBytes+bytes > 1<<20 {
		return errSlowConsumer
	}
	p.queueBytes += bytes
	return nil
}

func TestFakePeerVerticalContract(t *testing.T) {
	peer := newFakePeer()
	if err := peer.negotiate([]string{"1.0"}, []string{"terminal.v1", "health.v1"}); err != nil {
		t.Fatal(err)
	}
	if err := peer.negotiate([]string{"0.0"}, nil); !errors.Is(err, errProtocol) {
		t.Fatalf("unsupported version: %v", err)
	}
	if err := peer.negotiate([]string{"1.0"}, []string{"future.required"}); !errors.Is(err, errCapability) {
		t.Fatalf("missing capability: %v", err)
	}
	start, end, err := peer.attach(100)
	if !errors.Is(err, errReplayGap) || start != 1024 || end != 2048 {
		t.Fatalf("replay gap=(%d,%d,%v)", start, end, err)
	}
	if err := peer.stage("image.png"); err != nil {
		t.Fatal(err)
	}
	if err := peer.stage("../secret.png"); !errors.Is(err, errInvalidPath) {
		t.Fatalf("path traversal: %v", err)
	}
	if err := peer.preview(false); err != nil {
		t.Fatal(err)
	}
	if err := peer.preview(true); !errors.Is(err, errUnauthorized) {
		t.Fatalf("private preview: %v", err)
	}
	if err := peer.config(true, true, true); err != nil {
		t.Fatal(err)
	}
	if err := peer.config(true, true, false); !errors.Is(err, errUnauthorized) {
		t.Fatalf("config consent: %v", err)
	}
	if err := peer.cancel("op_1"); !errors.Is(err, errCanceled) || !peer.canceled["op_1"] {
		t.Fatalf("cancel: %v", err)
	}
	if err := peer.enqueue(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := peer.enqueue(1); !errors.Is(err, errSlowConsumer) {
		t.Fatalf("backpressure: %v", err)
	}
	peer.revoked = true
	if _, _, err := peer.attach(1024); !errors.Is(err, errUnauthorized) {
		t.Fatalf("revoked attach: %v", err)
	}
}
