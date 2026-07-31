package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

type Code string

const (
	Malformed        Code = "credential_malformed"
	AlgorithmInvalid Code = "credential_algorithm_invalid"
	KeyUnknown       Code = "credential_key_unknown"
	SignatureInvalid Code = "credential_signature_invalid"
	AudienceInvalid  Code = "credential_audience_invalid"
	ScopeInvalid     Code = "credential_scope_invalid"
	BindingInvalid   Code = "credential_binding_invalid"
	Expired          Code = "credential_expired"
	NotYetValid      Code = "credential_not_yet_valid"
	Revoked          Code = "credential_revoked"
	Replayed         Code = "credential_replayed"
)

type Error struct {
	Code  Code
	Cause error
}

func (e *Error) Error() string {
	if e.Cause == nil {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Cause.Error()
}
func (e *Error) Unwrap() error { return e.Cause }

type Claims struct {
	Issuer                 string              `json:"iss"`
	Audience               string              `json:"aud"`
	Subject                string              `json:"sub"`
	JTI                    string              `json:"jti"`
	IssuedAt               int64               `json:"iat"`
	ExpiresAt              int64               `json:"exp"`
	Scope                  []string            `json:"scope"`
	CredentialClass        string              `json:"credential_class"`
	EnvironmentID          string              `json:"environment_id,omitempty"`
	UserID                 string              `json:"user_id,omitempty"`
	CLIClientSessionID     string              `json:"cli_client_session_id,omitempty"`
	HelperID               string              `json:"helper_id,omitempty"`
	MachineID              string              `json:"machine_id,omitempty"`
	InstallationGeneration int64               `json:"installation_generation,omitempty"`
	SourceMachineID        string              `json:"source_machine_id,omitempty"`
	SessionID              string              `json:"session_id,omitempty"`
	AssignmentID           string              `json:"assignment_id,omitempty"`
	WarningRevision        string              `json:"warning_revision,omitempty"`
	ConnectorID            string              `json:"connector_id,omitempty"`
	ConnectorGeneration    uint64              `json:"connector_generation,omitempty"`
	EdgePool               string              `json:"edge_pool,omitempty"`
	EdgeNodeID             string              `json:"edge_node_id,omitempty"`
	FileTransferPolicy     *FileTransferPolicy `json:"file_transfer_policy,omitempty"`
	CounterEpoch           string              `json:"counter_epoch,omitempty"`
	Confirmation           *struct {
		JKT string `json:"jkt"`
	} `json:"cnf,omitempty"`
}

type FileTransferPolicy struct {
	Revision               string `json:"revision"`
	MaxFileBytes           int64  `json:"max_file_bytes"`
	MaxBatchFiles          int    `json:"max_batch_files"`
	MaxBatchBytes          int64  `json:"max_batch_bytes"`
	MaxConcurrentTransfers int    `json:"max_concurrent_transfers"`
	RetentionSeconds       int64  `json:"retention_seconds"`
	DeliveryTimeoutSeconds int64  `json:"delivery_timeout_seconds"`
	MaxPendingSpoolBytes   int64  `json:"max_pending_spool_bytes"`
}

type Policy struct {
	Issuer              string
	Audience            string
	CredentialClass     string
	Scopes              []string
	EnvironmentID       string
	UserID              string
	CLIClientSessionID  string
	HelperID            string
	MachineID           string
	SourceMachineID     string
	SessionID           string
	AssignmentID        string
	WarningRevision     string
	ConnectorID         string
	ConnectorGeneration uint64
	EdgePool            string
	EdgeNodeID          string
	CounterEpoch        string
	MaxLifetime         time.Duration
	SingleUse           bool
}

type Clock interface{ Now() time.Time }
type KeySource interface {
	Lookup(context.Context, string) (ed25519.PublicKey, bool, error)
	Refresh(context.Context) error
}
type Revocations interface{ Revoked(Claims) bool }
type ReplayStore interface{ Consume(string, time.Time) bool }

type Verifier struct {
	Keys           KeySource
	Clock          Clock
	Revocations    Revocations
	Replays        ReplayStore
	ClockSkew      time.Duration
	RefreshTimeout time.Duration
}

type header struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	Type      string `json:"typ"`
}

func (v Verifier) Verify(ctx context.Context, token string, policy Policy) (Claims, error) {
	if len(token) == 0 || len(token) > 16<<10 || v.Keys == nil || v.Clock == nil {
		return Claims{}, &Error{Code: Malformed}
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(parts[0]) > 2<<10 || len(parts[1]) > 12<<10 || len(parts[2]) > 512 {
		return Claims{}, &Error{Code: Malformed}
	}
	var h header
	if err := decodeSegment(parts[0], &h); err != nil {
		return Claims{}, &Error{Code: Malformed, Cause: err}
	}
	if h.Algorithm != "EdDSA" || h.Type != "paperboat-credential+jwt" || h.KeyID == "" {
		return Claims{}, &Error{Code: AlgorithmInvalid}
	}
	key, ok, err := v.Keys.Lookup(ctx, h.KeyID)
	if err != nil {
		return Claims{}, &Error{Code: KeyUnknown, Cause: err}
	}
	if !ok {
		timeout := v.RefreshTimeout
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		refreshCtx, cancel := context.WithTimeout(ctx, timeout)
		err = v.Keys.Refresh(refreshCtx)
		cancel()
		if err == nil {
			key, ok, err = v.Keys.Lookup(ctx, h.KeyID)
		}
	}
	if err != nil || !ok || len(key) != ed25519.PublicKeySize {
		return Claims{}, &Error{Code: KeyUnknown, Cause: err}
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != ed25519.SignatureSize {
		return Claims{}, &Error{Code: Malformed, Cause: err}
	}
	if !ed25519.Verify(key, []byte(parts[0]+"."+parts[1]), signature) {
		return Claims{}, &Error{Code: SignatureInvalid}
	}
	var claims Claims
	if err := decodeSegment(parts[1], &claims); err != nil {
		return Claims{}, &Error{Code: Malformed, Cause: err}
	}
	if claims.Issuer != policy.Issuer {
		return Claims{}, &Error{Code: BindingInvalid}
	}
	if claims.Audience != policy.Audience {
		return Claims{}, &Error{Code: AudienceInvalid}
	}
	if claims.CredentialClass != policy.CredentialClass || !bindingsMatch(claims, policy) || claims.UserID != "" && claims.Subject != claims.UserID {
		return Claims{}, &Error{Code: BindingInvalid}
	}
	if !exactScopes(claims.Scope, policy.Scopes) {
		return Claims{}, &Error{Code: ScopeInvalid}
	}
	now := v.Clock.Now()
	skew := v.ClockSkew
	if skew < 0 {
		skew = 0
	}
	issued := time.Unix(claims.IssuedAt, 0)
	expires := time.Unix(claims.ExpiresAt, 0)
	if claims.JTI == "" || claims.Subject == "" || claims.IssuedAt < 0 || claims.ExpiresAt <= claims.IssuedAt {
		return Claims{}, &Error{Code: Malformed}
	}
	if policy.MaxLifetime > 0 && expires.Sub(issued) > policy.MaxLifetime {
		return Claims{}, &Error{Code: BindingInvalid}
	}
	if now.After(expires.Add(skew)) {
		return Claims{}, &Error{Code: Expired}
	}
	if now.Add(skew).Before(issued) {
		return Claims{}, &Error{Code: NotYetValid}
	}
	if v.Revocations != nil && v.Revocations.Revoked(claims) {
		return Claims{}, &Error{Code: Revoked}
	}
	if policy.SingleUse {
		if v.Replays == nil || !v.Replays.Consume(claims.JTI, expires) {
			return Claims{}, &Error{Code: Replayed}
		}
	}
	return claims, nil
}

func decodeSegment(segment string, target any) error {
	b, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return err
	}
	if err := rejectDuplicateKeys(b); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var walk func() error
	walk = func() error {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for dec.More() {
				keyToken, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return errors.New("duplicate or invalid object key")
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := dec.Token()
			return err
		case '[':
			for dec.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := dec.Token()
			return err
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func bindingsMatch(c Claims, p Policy) bool {
	return match(p.EnvironmentID, c.EnvironmentID) && match(p.MachineID, c.MachineID) && match(p.SourceMachineID, c.SourceMachineID) && match(p.UserID, c.UserID) && match(p.CLIClientSessionID, c.CLIClientSessionID) && match(p.HelperID, c.HelperID) && match(p.SessionID, c.SessionID) && match(p.AssignmentID, c.AssignmentID) && match(p.WarningRevision, c.WarningRevision) && match(p.ConnectorID, c.ConnectorID) && matchUint(p.ConnectorGeneration, c.ConnectorGeneration) && match(p.EdgePool, c.EdgePool) && match(p.EdgeNodeID, c.EdgeNodeID) && match(p.CounterEpoch, c.CounterEpoch)
}
func match(expected, actual string) bool     { return expected == "" || expected == actual }
func matchUint(expected, actual uint64) bool { return expected == 0 || expected == actual }
func exactScopes(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	set := make(map[string]bool, len(expected))
	for _, scope := range expected {
		if scope == "" || set[scope] {
			return false
		}
		set[scope] = true
	}
	for _, scope := range actual {
		if !set[scope] {
			return false
		}
		delete(set, scope)
	}
	return len(set) == 0
}
