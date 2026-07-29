package nativeproto

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type fragmentReader struct{ data []byte }

type writeRecorder struct{ sizes []int }

func (w *writeRecorder) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	return len(p), nil
}

func (r *fragmentReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}

func TestPrefaceRoundTripWithFragmentation(t *testing.T) {
	var id [ConnectionIDSize]byte
	for i := range id {
		id[i] = byte(i + 1)
	}
	want := Preface{Role: RoleControl, ConnectionID: id, Token: "signed-token"}
	var encoded bytes.Buffer
	if err := WritePreface(&encoded, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPreface(&fragmentReader{data: encoded.Bytes()})
	if err != nil || got.Role != want.Role || got.ConnectionID != id || got.Token != want.Token || len(got.Binding) != 0 {
		t.Fatalf("got=%#v err=%v", got, err)
	}
}

func TestAuxiliaryPrefaceRoundTrip(t *testing.T) {
	var id [ConnectionIDSize]byte
	id[0] = 1
	binding := bytes.Repeat([]byte{7}, BindingSize)
	var encoded bytes.Buffer
	if err := WritePreface(&encoded, Preface{Role: RoleInput, ConnectionID: id, Binding: binding}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPreface(&encoded)
	if err != nil || !bytes.Equal(got.Binding, binding) {
		t.Fatalf("got=%#v err=%v", got, err)
	}
}

func TestPrefaceRejectsInvalidRoleAndVersion(t *testing.T) {
	var id [ConnectionIDSize]byte
	id[0] = 1
	if err := WritePreface(io.Discard, Preface{Role: 9, ConnectionID: id, Token: "x"}); !errors.Is(err, ErrPreface) {
		t.Fatalf("err=%v", err)
	}
	var encoded bytes.Buffer
	if err := WritePreface(&encoded, Preface{Role: RoleControl, ConnectionID: id, Token: "x"}); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	data[4]++
	if _, err := ReadPreface(bytes.NewReader(data)); !errors.Is(err, ErrPreface) {
		t.Fatalf("err=%v", err)
	}
}

func TestRecordsPreserveBoundariesUnderFragmentation(t *testing.T) {
	var encoded bytes.Buffer
	if err := WriteRecord(&encoded, RecordStructured, []byte("first"), true); err != nil {
		t.Fatal(err)
	}
	if err := WriteRecord(&encoded, RecordBinary, []byte("second"), true); err != nil {
		t.Fatal(err)
	}
	reader := &fragmentReader{data: encoded.Bytes()}
	for _, want := range []struct {
		kind byte
		data string
	}{{RecordStructured, "first"}, {RecordBinary, "second"}} {
		kind, data, err := ReadRecord(reader, true)
		if err != nil || kind != want.kind || string(data) != want.data {
			t.Fatalf("kind=%d data=%q err=%v", kind, data, err)
		}
	}
}

func TestRecordIsPresentedToWriterAsOneCompleteBuffer(t *testing.T) {
	writer := &writeRecorder{}
	if err := WriteRecord(writer, RecordBinary, []byte("payload"), true); err != nil {
		t.Fatal(err)
	}
	if len(writer.sizes) != 1 || writer.sizes[0] != 5+len("payload") {
		t.Fatalf("write sizes = %v", writer.sizes)
	}
}
