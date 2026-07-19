//go:build darwin || linux

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
)

type capacityProcess struct {
	done   chan struct{}
	once   sync.Once
	exited *atomic.Int32
}

func (p *capacityProcess) Read([]byte) (int, error) { <-p.done; return 0, io.EOF }
func (p *capacityProcess) Write(data []byte) (int, error) {
	select {
	case <-p.done:
		return 0, io.ErrClosedPipe
	default:
		return len(data), nil
	}
}
func (*capacityProcess) Resize(pty.Dimensions) error { return nil }
func (*capacityProcess) Signal(pty.Signal) error     { return nil }
func (p *capacityProcess) Wait(ctx context.Context) (pty.ExitResult, error) {
	select {
	case <-p.done:
		return pty.ExitResult{Code: 0, ExitedAt: time.Now().UTC()}, nil
	case <-ctx.Done():
		return pty.ExitResult{}, ctx.Err()
	}
}
func (p *capacityProcess) Terminate(ctx context.Context, _ time.Duration) (pty.ExitResult, error) {
	p.once.Do(func() { close(p.done); p.exited.Add(1) })
	return p.Wait(ctx)
}
func (p *capacityProcess) CloseIO() error {
	p.once.Do(func() { close(p.done); p.exited.Add(1) })
	return nil
}

func TestConfiguredSessionCapacityRemainsBoundedAndShutsDownCleanly(t *testing.T) {
	const maxSessions, maxAttachments, maxDecisions = 32, 4, 10
	var launched, exited atomic.Int32
	manager, err := NewManager(ManagerConfig{
		Launch: func(pty.Command) (PTYProcess, error) {
			launched.Add(1)
			return &capacityProcess{done: make(chan struct{}), exited: &exited}, nil
		},
		MaxSessions: maxSessions, MaxAttachments: maxAttachments, MaxInputDecisions: maxDecisions,
		HistoryBytes: 1024, AttachmentBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	var first Snapshot
	for index := 0; index < maxSessions; index++ {
		created, err := manager.Create(context.Background(), CreateRequest{Name: fmt.Sprintf("session-%02d", index), Command: pty.Command{Path: "/bin/sh", CWD: "/tmp", Dimensions: pty.Dimensions{Columns: 80, Rows: 24}}})
		if err != nil {
			t.Fatalf("create %d: %v", index, err)
		}
		if index == 0 {
			first = created
		}
	}
	if _, err := manager.Create(context.Background(), CreateRequest{Name: "overflow", Command: pty.Command{Path: "/bin/sh", CWD: "/tmp", Dimensions: pty.Dimensions{Columns: 80, Rows: 24}}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("session limit err=%v", err)
	}
	for index := 0; index < maxAttachments; index++ {
		if _, err := manager.Attach(first.ID, fmt.Sprintf("att-%d", index), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := manager.Attach(first.ID, "att-overflow", 0); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("attachment limit err=%v", err)
	}
	counts := manager.ResourceCounts()
	if counts["sessions"] != maxSessions || counts["processes"] != maxSessions || counts["attachments"] != maxAttachments {
		t.Fatalf("resource counts = %v", counts)
	}
	for index := 0; index < maxDecisions; index++ {
		key := InputKey{ClientID: "cli", AttachmentID: "att-0", Generation: first.Generation, InputID: fmt.Sprintf("input-%d", index)}
		if decision, err := manager.Write(first.ID, key, []byte("x")); err != nil || decision.Status != InputAccepted {
			t.Fatalf("input %d decision=%#v err=%v", index, decision, err)
		}
	}
	overflow := InputKey{ClientID: "cli", AttachmentID: "att-0", Generation: first.Generation, InputID: "input-overflow"}
	if _, err := manager.Write(first.ID, overflow, []byte("x")); !errors.Is(err, ErrInputJournalFull) {
		t.Fatalf("decision limit err=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for exited.Load() != maxSessions {
		select {
		case <-deadline.C:
			t.Fatalf("launched=%d exited=%d", launched.Load(), exited.Load())
		case <-ticker.C:
		}
	}
	if launched.Load() != maxSessions {
		t.Fatalf("launched=%d", launched.Load())
	}
}
