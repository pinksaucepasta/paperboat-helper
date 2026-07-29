package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	frpclient "github.com/fatedier/frp/client"
	v1 "github.com/fatedier/frp/pkg/config/v1"
)

type fakeRaceConnector struct {
	ctx    context.Context
	delay  time.Duration
	err    error
	closed atomic.Int32
}

func (c *fakeRaceConnector) Open() error {
	timer := time.NewTimer(c.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return c.err
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}
func (c *fakeRaceConnector) Connect() (net.Conn, error) { return nil, nil }
func (c *fakeRaceConnector) Close() error               { c.closed.Add(1); return nil }

func TestRacingConnectorSelectsTCPAfterSilentQUIC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &v1.ClientCommonConfig{}
	var quic, tcp *fakeRaceConnector
	r := newRacingConnector(ctx, cfg, t.TempDir()+"/preference.json").(*racingConnector)
	r.newConnector = func(candidate context.Context, config *v1.ClientCommonConfig) frpclient.Connector {
		value := &fakeRaceConnector{ctx: candidate}
		if config.Transport.Protocol == "quic" {
			value.delay = time.Second
			quic = value
		} else {
			value.delay = time.Millisecond
			tcp = value
		}
		return value
	}
	started := time.Now()
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 200*time.Millisecond || elapsed > 750*time.Millisecond {
		t.Fatalf("fallback elapsed=%s", elapsed)
	}
	if r.winner != tcp {
		t.Fatal("TCP did not win")
	}
	deadline := time.After(time.Second)
	for quic.closed.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("QUIC loser was not closed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestRacingConnectorReturnsAuthoritativeFailureOnlyAfterBothTransports(t *testing.T) {
	r := newRacingConnector(context.Background(), &v1.ClientCommonConfig{}, "").(*racingConnector)
	var attempts atomic.Int32
	r.newConnector = func(ctx context.Context, _ *v1.ClientCommonConfig) frpclient.Connector {
		attempts.Add(1)
		return &fakeRaceConnector{ctx: ctx, err: errors.New("unavailable")}
	}
	if err := r.Open(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
}

func TestRacingConnectorCloseCancelsCandidates(t *testing.T) {
	r := newRacingConnector(context.Background(), &v1.ClientCommonConfig{}, "").(*racingConnector)
	started := make(chan *fakeRaceConnector, 1)
	r.newConnector = func(ctx context.Context, _ *v1.ClientCommonConfig) frpclient.Connector {
		candidate := &fakeRaceConnector{ctx: ctx, delay: time.Hour}
		started <- candidate
		return candidate
	}
	done := make(chan error, 1)
	go func() { done <- r.Open() }()
	candidate := <-started
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Open did not stop after Close")
	}
	if candidate.closed.Load() == 0 {
		t.Fatal("candidate was not closed")
	}
}

func TestRacingConnectorIgnoresExpiredPreference(t *testing.T) {
	path := t.TempDir() + "/preference.json"
	data, _ := json.Marshal(transportPreference{Transport: TCPMux, ExpiresAt: time.Now().Add(-time.Second)})
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	r := newRacingConnector(context.Background(), &v1.ClientCommonConfig{}, path).(*racingConnector)
	selected := make(chan Transport, 2)
	r.newConnector = func(ctx context.Context, config *v1.ClientCommonConfig) frpclient.Connector {
		transport := TCPMux
		if config.Transport.Protocol == "quic" {
			transport = QUIC
		}
		selected <- transport
		return &fakeRaceConnector{ctx: ctx}
	}
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	if first := <-selected; first != QUIC {
		t.Fatalf("expired preference selected %q first", first)
	}
}

func TestRacingConnectorStartsTCPImmediatelyAfterQUICReject(t *testing.T) {
	r := newRacingConnector(context.Background(), &v1.ClientCommonConfig{}, "").(*racingConnector)
	var quic, tcp *fakeRaceConnector
	r.newConnector = func(ctx context.Context, config *v1.ClientCommonConfig) frpclient.Connector {
		candidate := &fakeRaceConnector{ctx: ctx}
		if config.Transport.Protocol == "quic" {
			candidate.err = errors.New("UDP rejected")
			quic = candidate
		} else {
			tcp = candidate
		}
		return candidate
	}
	started := time.Now()
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) >= 150*time.Millisecond {
		t.Fatal("immediate QUIC rejection waited for the fallback stagger")
	}
	if r.winner != tcp || quic.closed.Load() == 0 {
		t.Fatal("TCP did not replace rejected QUIC")
	}
}

func TestRacingConnectorRetriesQUICImmediatelyWhenPreferredTCPFails(t *testing.T) {
	path := t.TempDir() + "/preference.json"
	data, _ := json.Marshal(transportPreference{Transport: TCPMux, ExpiresAt: time.Now().Add(time.Hour)})
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	r := newRacingConnector(context.Background(), &v1.ClientCommonConfig{}, path).(*racingConnector)
	selected := make(chan Transport, 2)
	var quic *fakeRaceConnector
	r.newConnector = func(ctx context.Context, config *v1.ClientCommonConfig) frpclient.Connector {
		candidate := &fakeRaceConnector{ctx: ctx}
		transport := TCPMux
		if config.Transport.Protocol == "quic" {
			transport = QUIC
			quic = candidate
		} else {
			candidate.err = errors.New("TCP unavailable")
		}
		selected <- transport
		return candidate
	}
	started := time.Now()
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) >= 150*time.Millisecond {
		t.Fatal("preferred TCP failure waited for the fallback stagger")
	}
	if first, second := <-selected, <-selected; first != TCPMux || second != QUIC || r.winner != quic {
		t.Fatalf("attempt order=%q,%q winner=%T", first, second, r.winner)
	}
}

func TestRacingConnectorClosesOneOfSimultaneousSuccessfulCandidates(t *testing.T) {
	r := newRacingConnector(context.Background(), &v1.ClientCommonConfig{}, "").(*racingConnector)
	release := make(chan struct{})
	candidates := make(chan *fakeRaceConnector, 2)
	r.newConnector = func(ctx context.Context, _ *v1.ClientCommonConfig) frpclient.Connector {
		candidate := &fakeRaceConnector{ctx: ctx}
		candidate.delay = time.Hour
		candidates <- candidate
		return &blockingRaceConnector{fakeRaceConnector: candidate, release: release}
	}
	done := make(chan error, 1)
	go func() { done <- r.Open() }()
	first := <-candidates
	second := <-candidates
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for first.closed.Load()+second.closed.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("simultaneous-success loser was not closed")
		}
		time.Sleep(time.Millisecond)
	}
	if first.closed.Load()+second.closed.Load() != 1 {
		t.Fatalf("closed candidates=%d,%d", first.closed.Load(), second.closed.Load())
	}
}

type blockingRaceConnector struct {
	*fakeRaceConnector
	release <-chan struct{}
}

func (c *blockingRaceConnector) Open() error {
	select {
	case <-c.release:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}
