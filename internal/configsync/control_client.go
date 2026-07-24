// Package configsync owns the helper-side configuration synchronization
// runtime and its authenticated control-plane clients.
package configsync

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	ErrControlClientInvalid = errors.New("invalid config sync control client")
	ErrAuthorization        = errors.New("config sync authorization is unavailable")
	ErrLeaseBusy            = errors.New("config repository lease is busy")
	ErrLeaseLost            = errors.New("config repository lease is lost")
	ErrOperationConflict    = errors.New("config sync operation conflicts with an earlier request")
	ErrWritesDisabled       = errors.New("config repository writes are disabled")
)

const maxControlResponseBytes = 64 << 10

type TokenSource interface {
	Token(context.Context) (string, error)
}

type ProofSource interface {
	Proof(context.Context, string, string, string, []byte) ([]byte, error)
}

type OperationIDSource func() (string, error)

type ControlClientConfig struct {
	BaseURL         string
	AllowedHosts    []string
	RepositoryHosts []string
	Identities      TokenSource
	Proofs          ProofSource
	OperationID     OperationIDSource
	Transport       http.RoundTripper
	Timeout         time.Duration
	Clock           func() time.Time
}

type ControlClient struct {
	base            *url.URL
	identities      TokenSource
	proofs          ProofSource
	operationID     OperationIDSource
	client          *http.Client
	clock           func() time.Time
	repositoryHosts map[string]bool

	mu         sync.Mutex
	credential Credential
	access     RepositoryAccess
}

type Credential struct {
	Value           string    `json:"credential"`
	EnvironmentID   string    `json:"environment_id"`
	HelperID        string    `json:"helper_id"`
	AssignmentID    string    `json:"assignment_id"`
	WarningRevision string    `json:"warning_revision"`
	ExpiresAt       time.Time `json:"expires_at"`
}

type Lease struct {
	LeaseID       string    `json:"lease_id"`
	RepositoryID  string    `json:"repository_id"`
	AssignmentID  string    `json:"assignment_id"`
	EnvironmentID string    `json:"environment_id"`
	HelperID      string    `json:"helper_id"`
	FencingToken  int64     `json:"fencing_token"`
	BaseRevision  string    `json:"base_remote_revision"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type RepositoryAccess struct {
	RepositoryID  string    `json:"repository_id"`
	AssignmentID  string    `json:"assignment_id"`
	EnvironmentID string    `json:"environment_id"`
	HelperID      string    `json:"helper_id"`
	CloneURL      string    `json:"clone_url"`
	PublishURL    string    `json:"publish_url"`
	Branch        string    `json:"branch"`
	Username      string    `json:"username"`
	Password      string    `json:"password"`
	Capability    string    `json:"capability"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type RuntimePolicy struct {
	Format                  string        `json:"format"`
	Revision                string        `json:"revision"`
	Includes                []string      `json:"includes"`
	Excludes                []string      `json:"excludes"`
	MandatoryExclusions     []string      `json:"mandatory_exclusions"`
	MaxFileBytes            int64         `json:"max_file_bytes"`
	MaxBatchBytes           int64         `json:"max_batch_bytes"`
	Debounce                time.Duration `json:"debounce"`
	MinimumPushInterval     time.Duration `json:"minimum_push_interval"`
	MaximumDirtyDelay       time.Duration `json:"maximum_dirty_delay"`
	RemotePollInterval      time.Duration `json:"remote_poll_interval"`
	RetryLimit              int           `json:"retry_limit"`
	ShutdownFlushTimeout    time.Duration `json:"shutdown_flush_timeout"`
	SummaryLimit            int           `json:"summary_limit"`
	ClassifierEnabled       bool          `json:"classifier_enabled"`
	ClassifierRevision      string        `json:"classifier_revision"`
	ClassifierModelRevision string        `json:"classifier_model_revision"`
	RuntimeExclusionRoots   []string      `json:"-"`
}

type RuntimeDescriptor struct {
	WriteMode         string        `json:"write_mode"`
	RepositoryID      string        `json:"repository_id"`
	AssignmentID      string        `json:"assignment_id"`
	EnvironmentID     string        `json:"environment_id"`
	HelperID          string        `json:"helper_id"`
	HelperGeneration  int64         `json:"helper_generation"`
	SyncRevisionFloor int64         `json:"sync_revision_floor"`
	WarningRevision   string        `json:"warning_revision"`
	Policy            RuntimePolicy `json:"policy"`
	KeyVersion        int32         `json:"key_version"`
	AgeRecipient      string        `json:"age_recipient"`
	AgeIdentities     string        `json:"age_identities"`
}

func NewControlClient(config ControlClientConfig) (*ControlClient, error) {
	base, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || base.Scheme != "https" || base.User != nil || base.Hostname() == "" ||
		base.RawQuery != "" || base.Fragment != "" || config.Identities == nil || config.Proofs == nil ||
		config.OperationID == nil {
		return nil, ErrControlClientInvalid
	}
	allowed := false
	for _, host := range config.AllowedHosts {
		if strings.EqualFold(strings.TrimSpace(host), base.Hostname()) {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, ErrControlClientInvalid
	}
	if config.Timeout <= 0 {
		config.Timeout = 15 * time.Second
	}
	if config.Timeout > time.Minute {
		return nil, ErrControlClientInvalid
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	repositoryHosts := make(map[string]bool, len(config.RepositoryHosts))
	for _, host := range config.RepositoryHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@") {
			return nil, ErrControlClientInvalid
		}
		repositoryHosts[host] = true
	}
	transport := config.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &ControlClient{
		base: base, identities: config.Identities, proofs: config.Proofs, operationID: config.OperationID,
		client: &http.Client{
			Transport: transport, Timeout: config.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return ErrControlClientInvalid },
		},
		clock: config.Clock, repositoryHosts: repositoryHosts,
	}, nil
}

func (c *ControlClient) Credential(ctx context.Context) (Credential, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock().UTC()
	if c.credential.Value != "" && c.credential.ExpiresAt.After(now.Add(30*time.Second)) {
		return c.credential, nil
	}
	body := []byte("{}")
	path := "/v1/config/credentials"
	operationID, err := c.operationID()
	if err != nil {
		return Credential{}, errors.Join(ErrAuthorization, err)
	}
	identity, err := c.identities.Token(ctx)
	if err != nil {
		return Credential{}, errors.Join(ErrAuthorization, err)
	}
	proof, err := c.proofs.Proof(ctx, operationID, http.MethodPost, path, body)
	if err != nil {
		return Credential{}, errors.Join(ErrAuthorization, err)
	}
	request, err := c.request(ctx, path, body, identity, "", proof)
	if err != nil {
		return Credential{}, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return Credential{}, errors.Join(ErrAuthorization, err)
	}
	defer response.Body.Close()
	var envelope struct {
		Data struct {
			Credential      string    `json:"credential"`
			EnvironmentID   string    `json:"environment_id"`
			HelperID        string    `json:"helper_id"`
			AssignmentID    string    `json:"assignment_id"`
			WarningRevision string    `json:"warning_revision"`
			ExpiresAt       time.Time `json:"expires_at"`
		} `json:"data"`
	}
	if response.StatusCode != http.StatusOK || decodeBoundedJSON(response.Body, &envelope) != nil ||
		envelope.Data.Credential == "" || envelope.Data.EnvironmentID == "" || envelope.Data.HelperID == "" ||
		envelope.Data.AssignmentID == "" || envelope.Data.WarningRevision == "" ||
		!envelope.Data.ExpiresAt.After(now) || envelope.Data.ExpiresAt.After(now.Add(5*time.Minute+time.Second)) {
		return Credential{}, ErrAuthorization
	}
	c.credential = Credential{
		Value: envelope.Data.Credential, EnvironmentID: envelope.Data.EnvironmentID, HelperID: envelope.Data.HelperID,
		AssignmentID: envelope.Data.AssignmentID, WarningRevision: envelope.Data.WarningRevision, ExpiresAt: envelope.Data.ExpiresAt.UTC(),
	}
	return c.credential, nil
}

func (c *ControlClient) InvalidateCredential() {
	c.mu.Lock()
	c.credential = Credential{}
	c.access = RepositoryAccess{}
	c.mu.Unlock()
}

// RevalidateCredential forces a fresh eligibility decision without discarding
// repository access that is independently scoped and unexpired. Full
// invalidation still clears both layers after authorization loss or shutdown.
func (c *ControlClient) RevalidateCredential() {
	c.mu.Lock()
	c.credential = Credential{}
	c.mu.Unlock()
}

func (c *ControlClient) RepositoryAccess(ctx context.Context) (RepositoryAccess, error) {
	c.mu.Lock()
	now := c.clock().UTC()
	if c.access.Password != "" && c.access.ExpiresAt.After(now.Add(time.Minute)) {
		result := c.access
		c.mu.Unlock()
		return result, nil
	}
	c.mu.Unlock()
	operationID, err := c.operationID()
	if err != nil {
		return RepositoryAccess{}, err
	}
	body, err := json.Marshal(struct {
		OperationID string `json:"operation_id"`
	}{operationID})
	if err != nil {
		return RepositoryAccess{}, err
	}
	credential, err := c.Credential(ctx)
	if err != nil {
		return RepositoryAccess{}, err
	}
	identity, err := c.identities.Token(ctx)
	if err != nil {
		return RepositoryAccess{}, errors.Join(ErrAuthorization, err)
	}
	path := "/v1/config/repository-access"
	proof, err := c.proofs.Proof(ctx, operationID, http.MethodPost, path, body)
	if err != nil {
		return RepositoryAccess{}, errors.Join(ErrAuthorization, err)
	}
	request, err := c.request(ctx, path, body, identity, credential.Value, proof)
	if err != nil {
		return RepositoryAccess{}, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return RepositoryAccess{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusUnauthorized {
			c.InvalidateCredential()
			return RepositoryAccess{}, ErrAuthorization
		}
		return RepositoryAccess{}, ErrControlClientInvalid
	}
	var envelope struct {
		Data RepositoryAccess `json:"data"`
	}
	if decodeBoundedJSON(response.Body, &envelope) != nil || !c.validRepositoryAccess(envelope.Data, credential, now) {
		return RepositoryAccess{}, ErrControlClientInvalid
	}
	c.mu.Lock()
	c.access = envelope.Data
	c.mu.Unlock()
	return envelope.Data, nil
}

func (c *ControlClient) RuntimeDescriptor(ctx context.Context) (RuntimeDescriptor, error) {
	credential, err := c.Credential(ctx)
	if err != nil {
		return RuntimeDescriptor{}, err
	}
	operationID, err := c.operationID()
	if err != nil {
		return RuntimeDescriptor{}, err
	}
	body := []byte("{}")
	path := "/v1/config/runtime"
	identity, err := c.identities.Token(ctx)
	if err != nil {
		return RuntimeDescriptor{}, errors.Join(ErrAuthorization, err)
	}
	proof, err := c.proofs.Proof(ctx, operationID, http.MethodPost, path, body)
	if err != nil {
		return RuntimeDescriptor{}, errors.Join(ErrAuthorization, err)
	}
	request, err := c.request(ctx, path, body, identity, credential.Value, proof)
	if err != nil {
		return RuntimeDescriptor{}, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return RuntimeDescriptor{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusUnauthorized {
			c.InvalidateCredential()
			return RuntimeDescriptor{}, ErrAuthorization
		}
		return RuntimeDescriptor{}, ErrControlClientInvalid
	}
	var envelope struct {
		Data RuntimeDescriptor `json:"data"`
	}
	if decodeBoundedJSON(response.Body, &envelope) != nil || validateRuntimeDescriptor(envelope.Data, credential) != nil {
		return RuntimeDescriptor{}, ErrControlClientInvalid
	}
	return envelope.Data, nil
}

func (c *ControlClient) Classify(ctx context.Context, candidates []ClassificationCandidate) (ClassificationResponse, error) {
	if len(candidates) == 0 || len(candidates) > 256 {
		return ClassificationResponse{}, ErrClassificationInvalid
	}
	body, err := json.Marshal(struct {
		Candidates []ClassificationCandidate `json:"candidates"`
	}{Candidates: candidates})
	if err != nil {
		return ClassificationResponse{}, err
	}
	operationID, err := c.operationID()
	if err != nil {
		return ClassificationResponse{}, err
	}
	credential, err := c.Credential(ctx)
	if err != nil {
		return ClassificationResponse{}, err
	}
	identity, err := c.identities.Token(ctx)
	if err != nil {
		return ClassificationResponse{}, errors.Join(ErrAuthorization, err)
	}
	path := "/v1/config/classify"
	proof, err := c.proofs.Proof(ctx, operationID, http.MethodPost, path, body)
	if err != nil {
		return ClassificationResponse{}, errors.Join(ErrAuthorization, err)
	}
	request, err := c.request(ctx, path, body, identity, credential.Value, proof)
	if err != nil {
		return ClassificationResponse{}, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return ClassificationResponse{}, err
	}
	defer response.Body.Close()
	var envelope struct {
		Data ClassificationResponse `json:"data"`
	}
	if response.StatusCode != http.StatusOK || decodeBoundedJSON(response.Body, &envelope) != nil {
		return ClassificationResponse{}, ErrClassificationInvalid
	}
	return envelope.Data, nil
}

func (c *ControlClient) Pending(ctx context.Context) ([]ConflictResolution, error) {
	body := []byte("{}")
	path := "/v1/config/conflict-resolutions/pending"
	response, err := c.authorizedConfigRequest(ctx, path, body)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var envelope struct {
		Data struct {
			Items []ConflictResolution `json:"items"`
		} `json:"data"`
	}
	if response.StatusCode != http.StatusOK || decodeBoundedJSON(response.Body, &envelope) != nil ||
		len(envelope.Data.Items) > 100 {
		return nil, ErrControlClientInvalid
	}
	for _, item := range envelope.Data.Items {
		if !item.Valid() {
			return nil, ErrControlClientInvalid
		}
	}
	return envelope.Data.Items, nil
}

func (c *ControlClient) Acknowledge(ctx context.Context, id, landedRevision string) error {
	body, err := json.Marshal(struct {
		ID             string `json:"id"`
		LandedRevision string `json:"landed_revision"`
	}{ID: id, LandedRevision: landedRevision})
	if err != nil {
		return err
	}
	response, err := c.authorizedConfigRequest(ctx, "/v1/config/conflict-resolutions/acknowledge", body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var envelope struct {
		Data struct {
			Applied bool `json:"applied"`
		} `json:"data"`
	}
	if response.StatusCode != http.StatusOK || decodeBoundedJSON(response.Body, &envelope) != nil || !envelope.Data.Applied {
		return ErrControlClientInvalid
	}
	return nil
}

func (c *ControlClient) authorizedConfigRequest(ctx context.Context, path string, body []byte) (*http.Response, error) {
	operationID, err := c.operationID()
	if err != nil {
		return nil, err
	}
	credential, err := c.Credential(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := c.identities.Token(ctx)
	if err != nil {
		return nil, errors.Join(ErrAuthorization, err)
	}
	proof, err := c.proofs.Proof(ctx, operationID, http.MethodPost, path, body)
	if err != nil {
		return nil, errors.Join(ErrAuthorization, err)
	}
	request, err := c.request(ctx, path, body, identity, credential.Value, proof)
	if err != nil {
		return nil, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusUnauthorized {
		response.Body.Close()
		c.InvalidateCredential()
		return nil, ErrAuthorization
	}
	return response, nil
}

func (c *ControlClient) validRepositoryAccess(access RepositoryAccess, credential Credential, now time.Time) bool {
	if access.RepositoryID == "" || access.AssignmentID != credential.AssignmentID ||
		access.EnvironmentID != credential.EnvironmentID || access.HelperID != credential.HelperID ||
		access.Branch == "" || len(access.Branch) > 255 || access.Username != "x-access-token" ||
		(access.Capability != "repository_contents_read" && access.Capability != "repository_contents_write") ||
		access.Password == "" || len(access.Password) > 4096 || !access.ExpiresAt.After(now.Add(time.Minute)) ||
		access.ExpiresAt.After(now.Add(time.Hour+time.Minute)) {
		return false
	}
	for _, raw := range []string{access.CloneURL, access.PublishURL} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" ||
			parsed.RawQuery != "" || parsed.Fragment != "" || !strings.HasSuffix(parsed.Path, ".git") ||
			!c.repositoryHosts[strings.ToLower(parsed.Hostname())] {
			return false
		}
	}
	return true
}

func (c *ControlClient) AcquireLease(ctx context.Context, baseRevision string, ttl time.Duration) (Lease, error) {
	operationID, err := c.operationID()
	if err != nil {
		return Lease{}, err
	}
	return c.acquireLease(ctx, operationID, baseRevision, ttl)
}

func (c *ControlClient) acquireLease(ctx context.Context, operationID, baseRevision string, ttl time.Duration) (Lease, error) {
	baseRevision = strings.TrimSpace(baseRevision)
	body, err := json.Marshal(struct {
		OperationID        string `json:"operation_id"`
		BaseRemoteRevision string `json:"base_remote_revision"`
		TTLSeconds         int64  `json:"ttl_seconds"`
	}{operationID, baseRevision, int64(ttl / time.Second)})
	if err != nil {
		return Lease{}, err
	}
	lease, err := c.leaseRequest(ctx, "/v1/config/leases/acquire", operationID, body)
	if err == nil && lease.BaseRevision != baseRevision {
		return Lease{}, ErrControlClientInvalid
	}
	return lease, err
}

func (c *ControlClient) RenewLease(ctx context.Context, lease Lease, ttl time.Duration) (Lease, error) {
	operationID, err := c.operationID()
	if err != nil {
		return Lease{}, err
	}
	body, err := json.Marshal(struct {
		OperationID  string `json:"operation_id"`
		LeaseID      string `json:"lease_id"`
		FencingToken int64  `json:"fencing_token"`
		TTLSeconds   int64  `json:"ttl_seconds"`
	}{operationID, lease.LeaseID, lease.FencingToken, int64(ttl / time.Second)})
	if err != nil {
		return Lease{}, err
	}
	renewed, err := c.leaseRequest(ctx, "/v1/config/leases/renew", operationID, body)
	if err == nil && (renewed.LeaseID != lease.LeaseID || renewed.RepositoryID != lease.RepositoryID ||
		renewed.AssignmentID != lease.AssignmentID || renewed.EnvironmentID != lease.EnvironmentID ||
		renewed.HelperID != lease.HelperID || renewed.FencingToken != lease.FencingToken ||
		renewed.BaseRevision != lease.BaseRevision) {
		return Lease{}, ErrControlClientInvalid
	}
	return renewed, err
}

func (c *ControlClient) ReleaseLease(ctx context.Context, lease Lease) error {
	operationID, err := c.operationID()
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		OperationID  string `json:"operation_id"`
		LeaseID      string `json:"lease_id"`
		FencingToken int64  `json:"fencing_token"`
	}{operationID, lease.LeaseID, lease.FencingToken})
	if err != nil {
		return err
	}
	_, err = c.leaseRequest(ctx, "/v1/config/leases/release", operationID, body)
	return err
}

func (c *ControlClient) ReportStatus(ctx context.Context, status Status, summaryLimit int) error {
	if status.Validate(summaryLimit) != nil {
		return ErrControlClientInvalid
	}
	body, err := json.Marshal(status)
	if err != nil {
		return err
	}
	operationID, err := c.operationID()
	if err != nil {
		return err
	}
	path := "/v1/config/status"
	identity, err := c.identities.Token(ctx)
	if err != nil {
		return errors.Join(ErrAuthorization, err)
	}
	proof, err := c.proofs.Proof(ctx, operationID, http.MethodPost, path, body)
	if err != nil {
		return errors.Join(ErrAuthorization, err)
	}
	request, err := c.request(ctx, path, body, identity, "", proof)
	if err != nil {
		return err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var envelope struct {
		Data struct {
			SyncRevision int64 `json:"sync_revision"`
		} `json:"data"`
		Error struct {
			Code      string         `json:"code"`
			Message   string         `json:"message"`
			RequestID string         `json:"request_id"`
			Details   map[string]any `json:"details"`
		} `json:"error,omitempty"`
	}
	if decodeBoundedJSON(response.Body, &envelope) != nil {
		return ErrControlClientInvalid
	}
	if response.StatusCode == http.StatusConflict && envelope.Error.Code == "status_revision_stale" {
		return ErrOperationConflict
	}
	if response.StatusCode != http.StatusAccepted || envelope.Data.SyncRevision != status.SyncRevision {
		return ErrAuthorization
	}
	return nil
}

func (c *ControlClient) leaseRequest(ctx context.Context, path, operationID string, body []byte) (Lease, error) {
	credential, err := c.Credential(ctx)
	if err != nil {
		return Lease{}, err
	}
	identity, err := c.identities.Token(ctx)
	if err != nil {
		return Lease{}, errors.Join(ErrAuthorization, err)
	}
	proof, err := c.proofs.Proof(ctx, operationID, http.MethodPost, path, body)
	if err != nil {
		return Lease{}, errors.Join(ErrAuthorization, err)
	}
	request, err := c.request(ctx, path, body, identity, credential.Value, proof)
	if err != nil {
		return Lease{}, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return Lease{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		var envelope struct {
			Error struct {
				Code      string         `json:"code"`
				Message   string         `json:"message"`
				RequestID string         `json:"request_id"`
				Details   map[string]any `json:"details"`
			} `json:"error"`
		}
		_ = decodeBoundedJSON(response.Body, &envelope)
		switch envelope.Error.Code {
		case "config_writes_disabled":
			return Lease{}, ErrWritesDisabled
		case "lease_busy":
			return Lease{}, ErrLeaseBusy
		case "lease_lost":
			return Lease{}, ErrLeaseLost
		case "operation_conflict":
			return Lease{}, ErrOperationConflict
		default:
			if response.StatusCode == http.StatusUnauthorized {
				c.InvalidateCredential()
				return Lease{}, ErrAuthorization
			}
			return Lease{}, fmt.Errorf("config lease request failed with status %d", response.StatusCode)
		}
	}
	if path == "/v1/config/leases/release" {
		var envelope struct {
			Data struct {
				Released bool `json:"released"`
			} `json:"data"`
		}
		if err := decodeBoundedJSON(response.Body, &envelope); err != nil {
			return Lease{}, err
		}
		if !envelope.Data.Released {
			return Lease{}, ErrControlClientInvalid
		}
		return Lease{}, nil
	}
	var envelope struct {
		Data Lease `json:"data"`
	}
	if decodeBoundedJSON(response.Body, &envelope) != nil || envelope.Data.LeaseID == "" ||
		envelope.Data.RepositoryID == "" || envelope.Data.FencingToken < 1 ||
		envelope.Data.AssignmentID != credential.AssignmentID ||
		envelope.Data.EnvironmentID != credential.EnvironmentID || envelope.Data.HelperID != credential.HelperID ||
		!envelope.Data.ExpiresAt.After(c.clock().UTC()) {
		return Lease{}, ErrControlClientInvalid
	}
	return envelope.Data, nil
}

func (c *ControlClient) request(ctx context.Context, path string, body []byte, identity, credential string, proof []byte) (*http.Request, error) {
	endpoint := c.base.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	if credential == "" {
		request.Header.Set("Authorization", "Bearer "+identity)
	} else {
		request.Header.Set("Authorization", "Bearer "+credential)
		request.Header.Set("X-Paperboat-Helper-Identity", identity)
	}
	return request, nil
}

func decodeBoundedJSON(reader io.Reader, target any) error {
	data, err := io.ReadAll(io.LimitReader(reader, maxControlResponseBytes+1))
	if err != nil || len(data) > maxControlResponseBytes {
		return ErrControlClientInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrControlClientInvalid
	}
	return nil
}
