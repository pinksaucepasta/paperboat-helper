package preview

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type tokenSourceFunc func(context.Context) (string, error)

func (f tokenSourceFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestPreviewHTTPSenderCarriesStableIdentityRevisionAndFreshCredential(t *testing.T) {
	observation := Observation{Identity: "p-abcdefghijklmnopqrstuvwxyz", EnvironmentID: "env", LogicalName: "web", Target: Target{"127.0.0.1", 3000}, State: Ready, UpdatedAt: time.Now(), Revision: 4}
	tokens := 0
	var bodies, credentials []string
	proofs := controlProof(func(_ context.Context, _op, method, path string, body []byte) ([]byte, error) {
		if method != "POST" || path != "/preview" || len(body) == 0 {
			t.Fatal("invalid proof input")
		}
		return []byte("proof"), nil
	})
	sender, err := NewHTTPSender(HTTPSenderConfig{Endpoint: "https://api.test/preview", AllowedHosts: []string{"api.test"}, Tokens: tokenSourceFunc(func(context.Context) (string, error) { tokens++; return "token-" + string(rune('0'+tokens)), nil }), Identities: tokenSourceFunc(func(context.Context) (string, error) { return "identity", nil }), Proofs: proofs, OperationID: func() (string, error) { return "op_preview_test", nil }, Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(request.Body)
		bodies = append(bodies, string(body))
		credentials = append(credentials, request.Header.Get("Authorization"))
		if request.Header.Get("X-Paperboat-Helper-Identity") != "identity" || request.Header.Get("X-Paperboat-Preview-Identity") != observation.Identity || request.Header.Get("X-Paperboat-Preview-Revision") != "4" {
			return nil, errors.New("missing observation headers")
		}
		return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(context.Background(), observation); err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(context.Background(), observation); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] || credentials[0] == credentials[1] {
		t.Fatalf("bodies=%v credentials=%v", bodies, credentials)
	}
}

func TestPreviewHTTPSenderRejectsUnsafeEndpointStatusAndBounds(t *testing.T) {
	tokens := tokenSourceFunc(func(context.Context) (string, error) { return "token", nil })
	proofs := controlProof(func(context.Context, string, string, string, []byte) ([]byte, error) { return []byte("proof"), nil })
	for _, endpoint := range []string{"http://api.test/preview", "https://other.test/preview", "https://user@api.test/preview"} {
		if _, err := NewHTTPSender(HTTPSenderConfig{Endpoint: endpoint, AllowedHosts: []string{"api.test"}, Tokens: tokens, Identities: tokens, Proofs: proofs, OperationID: func() (string, error) { return "op_preview_test", nil }}); !errors.Is(err, ErrHTTPSenderInvalid) {
			t.Fatalf("endpoint=%s err=%v", endpoint, err)
		}
	}
	sender, err := NewHTTPSender(HTTPSenderConfig{Endpoint: "https://api.test/preview", AllowedHosts: []string{"api.test"}, Tokens: tokens, Identities: tokens, Proofs: proofs, OperationID: func() (string, error) { return "op_preview_test", nil }, MaxResponseBytes: 8, Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("123456789")), Request: request}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	err = sender.Send(context.Background(), Observation{Identity: "p-id", EnvironmentID: "env", LogicalName: "web", Target: Target{"127.0.0.1", 3000}, State: Registering, Revision: 1, UpdatedAt: time.Now()})
	if !errors.Is(err, ErrHTTPSenderInvalid) {
		t.Fatalf("err=%v", err)
	}
}
