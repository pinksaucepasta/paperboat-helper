package filetransfer

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

type loopbackRoundTripFunc func(*http.Request) (*http.Response, error)

func (f loopbackRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func loopbackResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

func TestLoopbackCreateRetriesResponseLossWithSameOperation(t *testing.T) {
	calls := 0
	client := &LoopbackClient{Endpoint: "http://127.0.0.1:1/v1/file-transfers", Token: "token", HTTPClient: &http.Client{Transport: loopbackRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		if request.Header.Get("X-Paperboat-Operation-ID") != "send_operation" {
			t.Errorf("operation ID = %q", request.Header.Get("X-Paperboat-Operation-ID"))
		}
		body, _ := io.ReadAll(request.Body)
		if string(body) != `{"batch_id":"batch"}` {
			t.Errorf("body = %q", body)
		}
		if calls == 1 {
			return nil, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset after commit")}
		}
		return loopbackResponse(request, http.StatusCreated, `{"transfers":[]}`), nil
	})}}
	var target struct {
		Transfers []store.FileTransfer `json:"transfers"`
	}
	if err := client.retryJSON(context.Background(), http.MethodPost, client.Endpoint, "send_operation", "application/json", 0, []byte(`{"batch_id":"batch"}`), &target); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestLoopbackTypedHTTPFailureDoesNotRetry(t *testing.T) {
	calls := 0
	client := &LoopbackClient{Endpoint: "http://127.0.0.1:1/v1/file-transfers", Token: "token", HTTPClient: &http.Client{Transport: loopbackRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		return loopbackResponse(request, http.StatusConflict, `{"code":"offset_conflict"}`), nil
	})}}
	if err := client.retryJSON(context.Background(), http.MethodPost, client.Endpoint, "send_operation", "application/json", 0, []byte(`{}`), nil); err == nil {
		t.Fatal("expected typed HTTP failure")
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestWaitReceiptSurvivesTransientHelperRestart(t *testing.T) {
	calls := 0
	client := &LoopbackClient{Endpoint: "http://127.0.0.1:1/v1/file-transfers", Token: "token", HTTPClient: &http.Client{Transport: loopbackRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("helper restarting")}
		}
		return loopbackResponse(request, http.StatusOK, `{"id":"ft_1","basename":"report.txt","state":"delivered","result_code":"stored","receipt_path":"Paperboat Inbox/report.txt"}`), nil
	})}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results, err := client.WaitReceipt(ctx, "send_operation", []store.FileTransfer{{ID: "ft_1", Basename: "report.txt", State: "pending"}})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(results) != 1 || results[0].State != "delivered" {
		t.Fatalf("calls=%d results=%#v", calls, results)
	}
}
