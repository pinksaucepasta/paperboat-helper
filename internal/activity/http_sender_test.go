package activity

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type tokenSourceFunc func(context.Context) (string, error)

func (f tokenSourceFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testBatch(t *testing.T) Batch {
	t.Helper()
	collector := deliveryCollector(t)
	batch, ok, err := collector.PeekBatch()
	if err != nil || !ok {
		t.Fatalf("batch=%#v ok=%v err=%v", batch, ok, err)
	}
	return batch
}

func TestHTTPSenderUsesFreshCredentialAndPreservesExactBatch(t *testing.T) {
	batch := testBatch(t)
	var mu sync.Mutex
	var bodies, tokens, ids []string
	tokenSequence := 0
	sender, err := NewHTTPSender(HTTPSenderConfig{Endpoint: "https://api.test/v1/activity", AllowedHosts: []string{"api.test"}, Tokens: tokenSourceFunc(func(context.Context) (string, error) {
		tokenSequence++
		return "token-" + string(rune('0'+tokenSequence)), nil
	}), Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(request.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		tokens = append(tokens, request.Header.Get("Authorization"))
		ids = append(ids, request.Header.Get("X-Paperboat-Activity-Batch-ID"))
		mu.Unlock()
		return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || bodies[0] != string(batch.Body) || bodies[1] != bodies[0] || tokens[0] == tokens[1] || ids[0] != ids[1] {
		t.Fatalf("bodies=%v tokens=%v ids=%v", bodies, tokens, ids)
	}
}

func TestHTTPSenderRejectsUnsafeEndpointStatusAndOversizedResponse(t *testing.T) {
	for _, endpoint := range []string{"http://api.test/activity", "https://other.test/activity", "https://user@api.test/activity"} {
		if _, err := NewHTTPSender(HTTPSenderConfig{Endpoint: endpoint, AllowedHosts: []string{"api.test"}, Tokens: tokenSourceFunc(func(context.Context) (string, error) { return "token", nil })}); !errors.Is(err, ErrHTTPSenderInvalid) {
			t.Fatalf("endpoint=%s err=%v", endpoint, err)
		}
	}
	status := http.StatusServiceUnavailable
	sender, err := NewHTTPSender(HTTPSenderConfig{Endpoint: "https://api.test/activity", AllowedHosts: []string{"api.test"}, MaxResponseBytes: 8, Tokens: tokenSourceFunc(func(context.Context) (string, error) { return "token", nil }), Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("123456789")), Request: request}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(context.Background(), testBatch(t)); !errors.Is(err, ErrHTTPSenderInvalid) {
		t.Fatalf("oversize err=%v", err)
	}
	status = http.StatusServiceUnavailable
	sender.maxResponseBytes = 64
	if err := sender.Send(context.Background(), testBatch(t)); !errors.Is(err, ErrHTTPSenderInvalid) {
		t.Fatalf("status err=%v", err)
	}
}

func TestHTTPSenderPropagatesCancellation(t *testing.T) {
	entered := make(chan struct{})
	sender, err := NewHTTPSender(HTTPSenderConfig{Endpoint: "https://api.test/activity", AllowedHosts: []string{"api.test"}, Tokens: tokenSourceFunc(func(context.Context) (string, error) { return "token", nil }), Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		close(entered)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = sender.Send(ctx, testBatch(t))
	<-entered
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}
