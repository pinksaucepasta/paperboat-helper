package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fetcherFunc func(context.Context) (io.ReadCloser, error)

func (f fetcherFunc) Fetch(ctx context.Context) (io.ReadCloser, error) { return f(ctx) }

func jwksFor(keyID string, key ed25519.PublicKey) string {
	return `{"keys":[{"kty":"OKP","crv":"Ed25519","use":"sig","alg":"EdDSA","kid":"` + keyID + `","x":"` + base64.RawURLEncoding.EncodeToString(key) + `"}]}`
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestHTTPJWKSFetcherRequiresAllowedHTTPSAndSuccessfulResponse(t *testing.T) {
	for _, endpoint := range []string{"http://keys.test/jwks", "https://other.test/jwks", "https://user@keys.test/jwks"} {
		if _, err := NewHTTPJWKSFetcher(endpoint, []string{"keys.test"}, roundTripperFunc(nil)); !errors.Is(err, ErrJWKSInvalid) {
			t.Fatalf("endpoint=%s err=%v", endpoint, err)
		}
	}
	var accept string
	fetcher, err := NewHTTPJWKSFetcher("https://keys.test/.well-known/jwks.json", []string{"keys.test"}, roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		accept = request.Header.Get("Accept")
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"keys":[]}`)), Request: request}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	body, err := fetcher.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	body.Close()
	if accept != "application/json" {
		t.Fatalf("accept=%q", accept)
	}

	fetcher.client.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("unavailable")), Request: request}, nil
	})
	if body, err := fetcher.Fetch(context.Background()); !errors.Is(err, ErrJWKSInvalid) || body != nil {
		t.Fatalf("body=%v err=%v", body, err)
	}
}

func TestJWKSRefreshRotationExpiryAndFailureRetention(t *testing.T) {
	clock := &fixedClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	first := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	first[0] = 1
	second := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	second[0] = 2
	body := jwksFor("key-1", first)
	cache, err := NewJWKSCache(JWKSConfig{Clock: clock, TTL: time.Minute, Fetcher: fetcherFunc(func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(body)), nil })})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if key, ok, err := cache.Lookup(context.Background(), "key-1"); err != nil || !ok || key[0] != 1 {
		t.Fatalf("key=%v ok=%v err=%v", key, ok, err)
	}
	clock.now = clock.now.Add(time.Minute)
	body = jwksFor("key-2", second)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if key, ok, _ := cache.Lookup(context.Background(), "key-1"); !ok || key[0] != 1 {
		t.Fatal("retired key was not retained through credential lifetime")
	}
	if key, ok, _ := cache.Lookup(context.Background(), "key-2"); !ok || key[0] != 2 {
		t.Fatalf("rotated key=%v ok=%v", key, ok)
	}
	body = `{"keys":[{"kid":"bad"}]}`
	if err := cache.Refresh(context.Background()); !errors.Is(err, ErrJWKSInvalid) {
		t.Fatalf("err=%v", err)
	}
	if _, ok, _ := cache.Lookup(context.Background(), "key-2"); !ok {
		t.Fatal("malformed refresh discarded last good keys")
	}
	clock.now = clock.now.Add(time.Minute)
	if _, ok, _ := cache.Lookup(context.Background(), "key-2"); ok {
		t.Fatal("expired key remained available")
	}
	clock.now = clock.now.Add(DefaultRetainMissing)
	if _, ok, _ := cache.Lookup(context.Background(), "key-1"); ok {
		t.Fatal("retired key remained beyond retention")
	}
}

func TestJWKSBoundsBodyKeysAndDuplicateFields(t *testing.T) {
	clock := &fixedClock{now: time.Now()}
	for name, testCase := range map[string]struct {
		body     string
		expected error
	}{
		"body":      {strings.Repeat("x", 65), ErrJWKSOverLimit},
		"duplicate": {`{"keys":[],"keys":[]}`, ErrJWKSInvalid},
		"empty":     {`{"keys":[]}`, ErrJWKSOverLimit},
	} {
		t.Run(name, func(t *testing.T) {
			cache, err := NewJWKSCache(JWKSConfig{Clock: clock, MaxBytes: 64, Fetcher: fetcherFunc(func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(testCase.body)), nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			if err := cache.Refresh(context.Background()); !errors.Is(err, testCase.expected) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestJWKSConcurrentRefreshesAreSerialized(t *testing.T) {
	clock := &fixedClock{now: time.Now()}
	key := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	var active, maximum atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	cache, err := NewJWKSCache(JWKSConfig{Clock: clock, Fetcher: fetcherFunc(func(context.Context) (io.ReadCloser, error) {
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		once.Do(func() { close(release) })
		<-release
		active.Add(-1)
		return io.NopCloser(strings.NewReader(jwksFor("key", key))), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := cache.Refresh(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wait.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("concurrent fetches=%d", maximum.Load())
	}
}

func TestJWKSPersistenceLoadsOnlyFreshValidatedMaterial(t *testing.T) {
	clock := &fixedClock{now: time.Now().UTC()}
	key := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	path := filepath.Join(t.TempDir(), "authorization", "jwks.json")
	cache, err := NewJWKSCache(JWKSConfig{Clock: clock, TTL: time.Minute, PersistencePath: path, Fetcher: fetcherFunc(func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(jwksFor("persisted", key))), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("persisted info=%v err=%v", info, err)
	}
	offline, err := NewJWKSCache(JWKSConfig{Clock: clock, TTL: time.Minute, PersistencePath: path, Fetcher: fetcherFunc(func(context.Context) (io.ReadCloser, error) {
		return nil, errors.New("offline")
	})})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := offline.Lookup(context.Background(), "persisted"); err != nil || !ok {
		t.Fatalf("fresh persisted key ok=%v err=%v", ok, err)
	}
	clock.now = clock.now.Add(time.Minute)
	stale, err := NewJWKSCache(JWKSConfig{Clock: clock, TTL: time.Minute, PersistencePath: path, Fetcher: fetcherFunc(func(context.Context) (io.ReadCloser, error) { return nil, errors.New("offline") })})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := stale.Lookup(context.Background(), "persisted"); ok {
		t.Fatal("stale persisted authorization material was accepted")
	}
}
