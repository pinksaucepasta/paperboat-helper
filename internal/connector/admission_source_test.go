package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
)

type identityTokenFunc func(context.Context) (string, error)

func (f identityTokenFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

type helperProofFunc func(context.Context, string, string, string, []byte) ([]byte, error)

func (f helperProofFunc) Proof(ctx context.Context, operationID, method, path string, body []byte) ([]byte, error) {
	return f(ctx, operationID, method, path, body)
}

type credentialVerifierFunc func(context.Context, string, auth.Policy) (auth.Claims, error)

func (f credentialVerifierFunc) Verify(ctx context.Context, token string, policy auth.Policy) (auth.Claims, error) {
	return f(ctx, token, policy)
}

type httpRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f httpRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func admissionSourceFor(t *testing.T, responseBody func(admissionRequest) string, verifier CredentialVerifier) *HTTPSAdmissionSource {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var operation atomic.Uint64
	source, err := NewHTTPSAdmissionSource(AdmissionSourceConfig{
		Endpoint: "https://api.test/v1/connectors/admission", AllowedHosts: []string{"api.test"}, Tokens: identityTokenFunc(func(context.Context) (string, error) { return "helper-identity", nil }), Proofs: helperProofFunc(func(_ context.Context, operationID, method, path string, body []byte) ([]byte, error) {
			if operationID == "" || method != http.MethodPost || path != "/v1/connectors/admission" || len(body) == 0 {
				return nil, errors.New("incorrect proof input")
			}
			return []byte("signed-helper-proof"), nil
		}), Verifier: verifier,
		Clock: fixedClock{now}, Issuer: "https://api.test", EnvironmentID: "env", HelperID: "helper", EdgePool: "default",
		OperationID: func() (string, error) { return "op_admit_000" + string(rune('0'+operation.Add(1))), nil },
		Transport: httpRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
			if request.Header.Get("Authorization") != "Bearer helper-identity" {
				return nil, errors.New("missing helper identity")
			}
			proof, err := base64.RawURLEncoding.DecodeString(request.Header.Get("X-Paperboat-Helper-Proof"))
			if err != nil || string(proof) != "signed-helper-proof" {
				return nil, errors.New("missing helper proof")
			}
			var input admissionRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(responseBody(input))), Request: request}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func validAdmissionResponse(input admissionRequest) string {
	encoded, _ := json.Marshal(admissionResponse{OperationID: input.OperationID, EnvironmentID: input.EnvironmentID, HelperID: input.HelperID, Generation: 3, EdgePool: input.EdgePool, EdgeNodeID: "edge_1", EdgeEndpoint: EdgeEndpoint{Host: "edge.test", Port: 7000}, Routes: []RouteHandoff{{RouteID: "route_1", Revision: 1, Kind: "helper_https_wss", PublicHost: "helper.test", ProxyName: "helper_1", LocalTarget: RouteTarget{Host: "127.0.0.1", Port: 8080}}}, ProtocolVersion: "1.0", Capabilities: []string{"terminal.v2"}, Credential: "test-only-connector-admission-credential"})
	return string(encoded)
}

func TestHTTPSAdmissionSourceVerifiesExactBindingsAndReturnsCredential(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	source := admissionSourceFor(t, validAdmissionResponse, credentialVerifierFunc(func(_ context.Context, token string, policy auth.Policy) (auth.Claims, error) {
		if token != "test-only-connector-admission-credential" || policy.Issuer != "https://api.test" || policy.Audience != "paperboat-edge" || policy.CredentialClass != "connector_admission" || policy.EnvironmentID != "env" || policy.HelperID != "helper" || policy.ConnectorGeneration != 3 || policy.EdgePool != "default" || policy.EdgeNodeID != "edge_1" || !policy.SingleUse {
			return auth.Claims{}, errors.New("incorrect policy")
		}
		return auth.Claims{JTI: "jti_admit_0001", EdgePool: "default", EdgeNodeID: "edge_1", ExpiresAt: now.Add(time.Minute).Unix()}, nil
	}))
	admission, err := source.Admission(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if admission.Credential != "test-only-connector-admission-credential" || admission.Generation != 3 || admission.JTI != "jti_admit_0001" {
		t.Fatalf("admission=%#v", admission)
	}
}

func TestHTTPSAdmissionSourceRejectsMalformedCrossBindingAndReplay(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	verifier := credentialVerifierFunc(func(context.Context, string, auth.Policy) (auth.Claims, error) {
		return auth.Claims{JTI: "jti", EdgePool: "default", EdgeNodeID: "edge_1", ExpiresAt: now.Add(time.Minute).Unix()}, nil
	})
	for name, response := range map[string]func(admissionRequest) string{
		"duplicate": func(input admissionRequest) string {
			valid := validAdmissionResponse(input)
			return strings.Replace(valid, `"environment_id":"env"`, `"environment_id":"env","environment_id":"env"`, 1)
		},
		"environment": func(input admissionRequest) string {
			input.EnvironmentID = "other"
			return validAdmissionResponse(input)
		},
		"capability": func(input admissionRequest) string {
			valid := validAdmissionResponse(input)
			return strings.Replace(valid, `"terminal.v2"`, `"BAD"`, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			source := admissionSourceFor(t, response, verifier)
			if _, err := source.Admission(context.Background()); !errors.Is(err, ErrAdmissionSourceInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	replayed := admissionSourceFor(t, validAdmissionResponse, credentialVerifierFunc(func(context.Context, string, auth.Policy) (auth.Claims, error) {
		return auth.Claims{}, &auth.Error{Code: auth.Replayed}
	}))
	if _, err := replayed.Admission(context.Background()); err == nil {
		t.Fatal("replayed credential accepted")
	}
}

func TestHTTPSAdmissionSourceRejectsUnsafeEndpointAndOversizedResponse(t *testing.T) {
	base := AdmissionSourceConfig{AllowedHosts: []string{"api.test"}, Tokens: identityTokenFunc(func(context.Context) (string, error) { return "token", nil }), Proofs: helperProofFunc(func(context.Context, string, string, string, []byte) ([]byte, error) { return []byte("proof"), nil }), Verifier: credentialVerifierFunc(func(context.Context, string, auth.Policy) (auth.Claims, error) { return auth.Claims{}, nil }), Clock: fixedClock{time.Now()}, Issuer: "https://api.test", EnvironmentID: "env", HelperID: "helper", EdgePool: "default", OperationID: func() (string, error) { return "op_admit_0001", nil }}
	for _, endpoint := range []string{"http://api.test/admit", "https://other.test/admit", "https://user@api.test/admit"} {
		config := base
		config.Endpoint = endpoint
		if _, err := NewHTTPSAdmissionSource(config); !errors.Is(err, ErrAdmissionSourceInvalid) {
			t.Fatalf("endpoint=%s err=%v", endpoint, err)
		}
	}
	source := admissionSourceFor(t, func(admissionRequest) string { return strings.Repeat("x", 65<<10) }, base.Verifier)
	if _, err := source.Admission(context.Background()); !errors.Is(err, ErrAdmissionSourceInvalid) {
		t.Fatalf("oversize err=%v", err)
	}
}
