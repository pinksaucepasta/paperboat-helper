package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/nativeproto"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

func nativeTestConnection(t *testing.T) (*nativeConnection, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	var id [nativeproto.ConnectionIDSize]byte
	id[0] = 1
	connection := newNativeConnection(server, id, 20*time.Millisecond)
	for i := range connection.binding {
		connection.binding[i] = byte(i + 1)
	}
	return connection, client
}

func TestNativeThreeStreamAssociationAuthenticatesOnce(t *testing.T) {
	protocolServer := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "principal"}, nil
	}, func(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
		return operation.Outcome{}
	}, 2)
	limiter, _ := NewConnectionLimiter(3)
	manager, _ := NewNativeAssociationManager(NativeAssociationConfig{Server: protocolServer, Authorizer: func(token string) (Authorizer, error) {
		if token != "signed-token" {
			return nil, ErrNativeAssociation
		}
		return authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
			return Authorization{JournalBinding: "principal"}, nil
		}), nil
	}, Limiter: limiter, Expiry: time.Second, Random: bytes.NewReader(bytes.Repeat([]byte{7}, nativeproto.BindingSize))})
	var id [nativeproto.ConnectionIDSize]byte
	id[0] = 1
	controlServer, controlClient := net.Pipe()
	defer controlClient.Close()
	controlDone := make(chan error, 1)
	go func() { controlDone <- manager.Serve(controlServer) }()
	if err := nativeproto.WritePreface(controlClient, nativeproto.Preface{Role: nativeproto.RoleControl, ConnectionID: id, Token: "signed-token"}); err != nil {
		t.Fatal(err)
	}
	hello := protocol.Frame{Type: "hello", RequestID: "req_hello", Version: protocol.ProtocolVersion, Payload: json.RawMessage(`{"min_version":"2.0","max_version":"2.0","capabilities":["terminal.v2","health.v1"]}`)}
	encoded, _ := json.Marshal(hello)
	if err := nativeproto.WriteRecord(controlClient, nativeproto.RecordStructured, encoded, true); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := nativeproto.ReadRecord(controlClient, true)
	if err != nil || kind != nativeproto.RecordStructured {
		t.Fatalf("welcome kind=%d err=%v", kind, err)
	}
	var welcome protocol.Frame
	if json.Unmarshal(payload, &welcome) != nil || welcome.Type != "welcome" {
		t.Fatalf("welcome=%s", payload)
	}
	var fields struct {
		Binding []byte `json:"binding_secret"`
	}
	if json.Unmarshal(welcome.Payload, &fields) != nil || len(fields.Binding) != nativeproto.BindingSize {
		t.Fatalf("welcome payload=%s", welcome.Payload)
	}
	attach := func(role byte) (net.Conn, <-chan error) {
		serverSide, clientSide := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- manager.Serve(serverSide) }()
		if err := nativeproto.WritePreface(clientSide, nativeproto.Preface{Role: role, ConnectionID: id, Binding: fields.Binding}); err != nil {
			t.Fatal(err)
		}
		return clientSide, done
	}
	input, inputDone := attach(nativeproto.RoleInput)
	defer input.Close()
	output, outputDone := attach(nativeproto.RoleOutput)
	defer output.Close()
	select {
	case err := <-inputDone:
		t.Fatalf("input rejected immediately after welcome: %v", err)
	case err := <-outputDone:
		t.Fatalf("output rejected immediately after welcome: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	request := protocol.Frame{Type: "request", RequestID: "req_health", Version: protocol.ProtocolVersion, OperationID: "op_health_0001", Capability: "health.v1", DeadlineMS: 1000, Payload: json.RawMessage(`{}`)}
	encoded, _ = json.Marshal(request)
	if err := nativeproto.WriteRecord(controlClient, nativeproto.RecordStructured, encoded, true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := nativeproto.ReadRecord(controlClient, true); err != nil {
		t.Fatal(err)
	}
	_ = controlClient.Close()
	select {
	case <-controlDone:
	case <-time.After(time.Second):
		t.Fatal("control did not close")
	}
	select {
	case <-inputDone:
	case <-time.After(time.Second):
		t.Fatal("input did not close")
	}
	select {
	case <-outputDone:
	case <-time.After(time.Second):
		t.Fatal("output did not close")
	}
}

func TestNativeAssociationRejectsDuplicateRoles(t *testing.T) {
	connection, control := nativeTestConnection(t)
	defer control.Close()
	defer connection.Close()
	connection.markAuthenticated()
	inputA, peerA := net.Pipe()
	defer peerA.Close()
	inputB, peerB := net.Pipe()
	defer inputB.Close()
	defer peerB.Close()
	if !connection.attach(nativeproto.RoleInput, inputA) {
		t.Fatal("first input rejected")
	}
	if connection.attach(nativeproto.RoleInput, inputB) {
		t.Fatal("duplicate input accepted")
	}
}

func TestNativeAssociationRejectsAuxiliaryBeforeControlAuthentication(t *testing.T) {
	connection, control := nativeTestConnection(t)
	defer control.Close()
	defer connection.Close()
	input, peer := net.Pipe()
	defer input.Close()
	defer peer.Close()
	if connection.attach(nativeproto.RoleInput, input) {
		t.Fatal("input attached before control authentication")
	}
}

func TestNativeAssociationRequiresExactBinding(t *testing.T) {
	connection, control := nativeTestConnection(t)
	defer control.Close()
	defer connection.Close()
	manager := &NativeAssociationManager{sets: map[[nativeproto.ConnectionIDSize]byte]*nativeConnection{connection.id: connection}}
	server, client := net.Pipe()
	defer client.Close()
	preface := nativeproto.Preface{Role: nativeproto.RoleInput, ConnectionID: connection.id, Binding: bytes.Repeat([]byte{9}, nativeproto.BindingSize)}
	if err := manager.serveAuxiliary(server, preface); err == nil {
		t.Fatal("wrong binding accepted")
	}
}

func TestNativeIncompleteAssociationExpires(t *testing.T) {
	connection, control := nativeTestConnection(t)
	defer control.Close()
	connection.mu.Lock()
	connection.timer = time.AfterFunc(10*time.Millisecond, connection.expireIncomplete)
	connection.mu.Unlock()
	select {
	case <-connection.done:
	case <-time.After(time.Second):
		t.Fatal("incomplete association did not expire")
	}
}

func TestNativeControlCloseClosesAuxiliaryStreams(t *testing.T) {
	connection, control := nativeTestConnection(t)
	input, inputPeer := net.Pipe()
	output, outputPeer := net.Pipe()
	defer control.Close()
	defer inputPeer.Close()
	defer outputPeer.Close()
	connection.markAuthenticated()
	if !connection.attach(nativeproto.RoleInput, input) || !connection.attach(nativeproto.RoleOutput, output) {
		t.Fatal("attach failed")
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := inputPeer.Write([]byte("x")); err == nil {
		t.Fatal("input remained open")
	}
	if _, err := outputPeer.Write([]byte("x")); err == nil {
		t.Fatal("output remained open")
	}
}
