package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

type terminalSecurityHandler struct {
	inputs  int
	acks    int
	resizes int
}

func (h *terminalSecurityHandler) Handle(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
	return operation.Outcome{}
}
func (h *terminalSecurityHandler) HandleTerminalInput(context.Context, Authorization, string, string, uint64, []byte) error {
	h.inputs++
	return nil
}
func (h *terminalSecurityHandler) HandleTerminalACK(context.Context, Authorization, string, string, uint64) error {
	h.acks++
	return nil
}
func (h *terminalSecurityHandler) HandleTerminalResize(context.Context, Authorization, string, string, uint16, uint16) error {
	h.resizes++
	return nil
}

func boundTerminalState(streamID uint32) *terminalConnectionState {
	state := newTerminalConnectionState()
	state.streams[streamID] = &terminalStreamBinding{authorization: Authorization{ClientID: "cli_1"}, sessionID: "ses_1", attachmentID: "att_1", generation: 1}
	return state
}

func requireInvalidTerminalFrame(t *testing.T, err error) {
	t.Helper()
	var protocolErr *protocol.Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != protocol.InvalidFrame {
		t.Fatalf("error=%v, want invalid terminal frame", err)
	}
}

func TestTerminalStreamIDsCannotCrossConnections(t *testing.T) {
	handler := &terminalSecurityHandler{}
	server := &Server{config: Config{Handler: handler}}
	first := boundTerminalState(7)
	second := boundTerminalState(9)
	message, _ := protocol.EncodeTerminalInput(protocol.TerminalInputFrame{StreamID: 7, Sequence: 1, Data: []byte("x")}, nil)
	if err := server.handleTerminalData(context.Background(), message, first); err != nil {
		t.Fatal(err)
	}
	requireInvalidTerminalFrame(t, server.handleTerminalData(context.Background(), message, second))
	if handler.inputs != 1 {
		t.Fatalf("input calls=%d", handler.inputs)
	}
}

func TestTerminalInputAndResizeSequencesMustBeContiguous(t *testing.T) {
	handler := &terminalSecurityHandler{}
	server := &Server{config: Config{Handler: handler}}
	state := boundTerminalState(7)

	input, _ := protocol.EncodeTerminalInput(protocol.TerminalInputFrame{StreamID: 7, Sequence: 1, Data: []byte("x")}, nil)
	if err := server.handleTerminalData(context.Background(), input, state); err != nil {
		t.Fatal(err)
	}
	requireInvalidTerminalFrame(t, server.handleTerminalData(context.Background(), input, state))
	gap, _ := protocol.EncodeTerminalInput(protocol.TerminalInputFrame{StreamID: 7, Sequence: 3, Data: []byte("x")}, nil)
	requireInvalidTerminalFrame(t, server.handleTerminalData(context.Background(), gap, state))

	resize, _ := protocol.EncodeTerminalResize(protocol.TerminalResizeFrame{StreamID: 7, Columns: 120, Rows: 40, Sequence: 1}, nil)
	if err := server.handleTerminalData(context.Background(), resize, state); err != nil {
		t.Fatal(err)
	}
	requireInvalidTerminalFrame(t, server.handleTerminalData(context.Background(), resize, state))
	resizeGap, _ := protocol.EncodeTerminalResize(protocol.TerminalResizeFrame{StreamID: 7, Columns: 80, Rows: 24, Sequence: 3}, nil)
	requireInvalidTerminalFrame(t, server.handleTerminalData(context.Background(), resizeGap, state))

	if handler.inputs != 1 || handler.resizes != 1 {
		t.Fatalf("input calls=%d resize calls=%d", handler.inputs, handler.resizes)
	}
}

func TestRevokedTerminalBindingRejectsDataFrames(t *testing.T) {
	handler := &terminalSecurityHandler{}
	server := &Server{config: Config{Handler: handler}}
	state := boundTerminalState(7)
	flag := new(atomic.Bool)
	state.streams[7].revoked = flag
	flag.Store(true)

	input, _ := protocol.EncodeTerminalInput(protocol.TerminalInputFrame{StreamID: 7, Sequence: 1, Data: []byte("x")}, nil)
	ack, _ := protocol.EncodeTerminalACK(protocol.TerminalACKFrame{StreamID: 7, NextSequence: 1}, nil)
	resize, _ := protocol.EncodeTerminalResize(protocol.TerminalResizeFrame{StreamID: 7, Columns: 80, Rows: 24, Sequence: 1}, nil)
	for _, frame := range [][]byte{input, ack, resize} {
		var protocolErr *protocol.Error
		err := server.handleTerminalData(context.Background(), frame, state)
		if !errors.As(err, &protocolErr) || protocolErr.Code != protocol.CredentialExpired {
			t.Fatalf("error=%v, want credential expired", err)
		}
	}
	if handler.inputs != 0 || handler.acks != 0 || handler.resizes != 0 {
		t.Fatalf("handler calls: input=%d ack=%d resize=%d", handler.inputs, handler.acks, handler.resizes)
	}
}
