package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	"github.com/pinksaucepasta/paperboat-helper/internal/connector"
	"github.com/pinksaucepasta/paperboat-helper/internal/enrollment"
)

var errInvalid = errors.New("invalid helper control conformance configuration")

type config struct {
	ControlURL    string `json:"control_url"`
	ControlCAFile string `json:"control_ca_file"`
	StateRoot     string `json:"state_root"`
	Issuer        string `json:"issuer"`
}

type clock struct{}

func (clock) Now() time.Time { return time.Now().UTC() }

type replayStore struct {
	mu   sync.Mutex
	jtis map[string]time.Time
}

func (s *replayStore) Consume(jti string, expires time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jtis[jti]; exists {
		return false
	}
	s.jtis[jti] = expires
	return true
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: paperboat-helper-control-conformance <absolute-private-config-path>")
		os.Exit(2)
	}
	if err := run(context.Background(), os.Args[1], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "paperboat-helper-control-conformance: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, path string, output io.Writer) error {
	value, err := loadConfig(path)
	if err != nil {
		return err
	}
	identity, err := enrollment.LoadRuntimeIdentity(value.StateRoot, time.Now().UTC())
	if err != nil {
		return err
	}
	ca, err := os.ReadFile(value.ControlCAFile)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errInvalid
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}}
	base, err := url.Parse(value.ControlURL)
	if err != nil || base.Scheme != "https" || base.Hostname() == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return errInvalid
	}
	fetcher, err := auth.NewHTTPJWKSFetcher(base.ResolveReference(&url.URL{Path: "/.well-known/jwks.json"}).String(), []string{base.Hostname()}, transport)
	if err != nil {
		return err
	}
	cache, err := auth.NewJWKSCache(auth.JWKSConfig{Fetcher: fetcher, Clock: clock{}, TTL: time.Minute, RetainMissing: 61 * time.Minute})
	if err != nil || cache.Refresh(ctx) != nil {
		return errors.Join(errInvalid, err)
	}
	verifier := auth.Verifier{Keys: cache, Clock: clock{}, Replays: &replayStore{jtis: make(map[string]time.Time)}, ClockSkew: 30 * time.Second}
	source, err := connector.NewHTTPSAdmissionSource(connector.AdmissionSourceConfig{
		Endpoint: base.ResolveReference(&url.URL{Path: "/v1/connectors/admission"}).String(), AllowedHosts: []string{base.Hostname()},
		Tokens: enrollment.TokenSource{StateRoot: value.StateRoot}, Proofs: enrollment.ProofSource{StateRoot: value.StateRoot}, Verifier: verifier,
		Clock: clock{}, Issuer: value.Issuer, EnvironmentID: identity.EnvironmentID, HelperID: identity.HelperID, EdgePool: "default",
		OperationID: func() (string, error) { return "op_helper_conformance_admission_0001", nil }, Transport: transport,
	})
	if err != nil {
		return err
	}
	admission, err := source.Admission(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"status": "passed", "helper_id": admission.HelperID, "environment_id": admission.EnvironmentID, "edge_node_id": admission.EdgeNodeID, "connector_generation": admission.Generation, "route_count": len(admission.Routes)})
}

func loadConfig(path string) (config, error) {
	if !filepath.IsAbs(path) {
		return config{}, errInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() > 32<<10 {
		return config{}, errInvalid
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return config{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var value config
	if decoder.Decode(&value) != nil {
		return config{}, errInvalid
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF || value.ControlURL == "" || !filepath.IsAbs(value.ControlCAFile) || !filepath.IsAbs(value.StateRoot) || value.Issuer == "" {
		return config{}, errInvalid
	}
	return value, nil
}
