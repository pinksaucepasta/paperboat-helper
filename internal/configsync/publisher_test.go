package configsync

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type fakeLeaseAuthority struct {
	events        *[]string
	acquireBases  []string
	nextFence     int64
	expiresAt     time.Time
	credentialErr error
	renewErr      error
}

func (a *fakeLeaseAuthority) Credential(context.Context) (Credential, error) {
	*a.events = append(*a.events, "credential")
	if a.credentialErr != nil {
		return Credential{}, a.credentialErr
	}
	return Credential{Value: "credential", AssignmentID: "assignment", WarningRevision: "warning"}, nil
}
func (a *fakeLeaseAuthority) AcquireLease(_ context.Context, base string, _ time.Duration) (Lease, error) {
	*a.events = append(*a.events, "acquire:"+base)
	a.acquireBases = append(a.acquireBases, base)
	a.nextFence++
	expiresAt := a.expiresAt
	if expiresAt.IsZero() {
		expiresAt = time.Now().Add(time.Minute)
	}
	return Lease{LeaseID: "lease", BaseRevision: base, FencingToken: a.nextFence, ExpiresAt: expiresAt}, nil
}
func (a *fakeLeaseAuthority) RenewLease(_ context.Context, lease Lease, _ time.Duration) (Lease, error) {
	*a.events = append(*a.events, "renew")
	if a.renewErr != nil {
		return Lease{}, a.renewErr
	}
	return lease, nil
}
func (a *fakeLeaseAuthority) ReleaseLease(_ context.Context, lease Lease) error {
	*a.events = append(*a.events, "release:"+lease.BaseRevision)
	return nil
}

type fakeRepository struct {
	events       *[]string
	fetches      []RemoteSnapshot
	prepared     PreparedPublication
	reconcileErr error
	published    PublishResult
	publishErr   error
	observed     bool
	observedHead string
	observeErr   error
	publishFence int64
	publishBlock bool
}

type observingRepository struct {
	*fakeRepository
	committed []string
}

func (r *observingRepository) PublicationCommitted(_ context.Context, prepared PreparedPublication, revision string) error {
	r.committed = append(r.committed, prepared.CommitID+":"+revision)
	return nil
}

func (r *fakeRepository) Fetch(context.Context) (RemoteSnapshot, error) {
	*r.events = append(*r.events, "fetch")
	if len(r.fetches) == 0 {
		return RemoteSnapshot{}, errors.New("unexpected fetch")
	}
	value := r.fetches[0]
	r.fetches = r.fetches[1:]
	return value, nil
}
func (r *fakeRepository) Reconcile(_ context.Context, snapshot RemoteSnapshot) (PreparedPublication, error) {
	*r.events = append(*r.events, "reconcile:"+snapshot.Revision)
	return r.prepared, r.reconcileErr
}
func (r *fakeRepository) Publish(ctx context.Context, _ PreparedPublication, fence int64) (PublishResult, error) {
	*r.events = append(*r.events, "publish")
	r.publishFence = fence
	if r.publishBlock {
		<-ctx.Done()
		return PublishResult{Uncertain: true}, ctx.Err()
	}
	return r.published, r.publishErr
}
func (r *fakeRepository) ObserveCommit(context.Context, string) (bool, string, error) {
	*r.events = append(*r.events, "observe")
	return r.observed, r.observedHead, r.observeErr
}

func TestPublisherEnforcesPullReconcileRevalidateAndCASOrder(t *testing.T) {
	events := []string{}
	authority := &fakeLeaseAuthority{events: &events}
	repository := &fakeRepository{
		events: &events, fetches: []RemoteSnapshot{{Revision: "head-1"}, {Revision: "head-1"}, {Revision: "head-1"}},
		prepared:  PreparedPublication{ExpectedRemoteRevision: "head-1", CommitID: "commit-2", HasChanges: true},
		published: PublishResult{RemoteRevision: "commit-2", Landed: true},
	}
	publisher, err := NewPublisher(PublisherConfig{Authority: authority, Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	result, err := publisher.Sync(context.Background(), "head-1")
	if err != nil || !result.Landed || result.RemoteRevision != "commit-2" {
		t.Fatalf("result = %#v, %v", result, err)
	}
	want := []string{"fetch", "credential", "reconcile:head-1", "acquire:head-1", "fetch", "reconcile:head-1", "renew", "credential", "fetch", "publish", "release:head-1"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	if repository.publishFence != 1 {
		t.Fatalf("publish fencing token = %d", repository.publishFence)
	}
}

func TestPublisherLeasesTheFreshlyObservedRemoteHead(t *testing.T) {
	events := []string{}
	authority := &fakeLeaseAuthority{events: &events}
	repository := &fakeRepository{
		events:    &events,
		fetches:   []RemoteSnapshot{{Revision: "head-2"}, {Revision: "head-2"}, {Revision: "head-2"}},
		prepared:  PreparedPublication{ExpectedRemoteRevision: "head-2", CommitID: "commit-3", HasChanges: true},
		published: PublishResult{RemoteRevision: "commit-3", Landed: true},
	}
	publisher, _ := NewPublisher(PublisherConfig{Authority: authority, Repository: repository})
	if _, err := publisher.Sync(context.Background(), "head-1"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(authority.acquireBases, []string{"head-2"}) || repository.publishFence != 1 {
		t.Fatalf("acquire bases = %#v, fence = %d", authority.acquireBases, repository.publishFence)
	}
}

func TestPublisherAppliesReadOnlyPullWithoutWriterLease(t *testing.T) {
	events := []string{}
	authority := &fakeLeaseAuthority{events: &events}
	repository := &fakeRepository{
		events: &events, fetches: []RemoteSnapshot{{Revision: "head"}},
		prepared: PreparedPublication{ExpectedRemoteRevision: "head", CommitID: "head"},
	}
	publisher, _ := NewPublisher(PublisherConfig{Authority: authority, Repository: repository})
	result, err := publisher.Sync(context.Background(), "")
	if err != nil || !result.Landed || result.RemoteRevision != "head" {
		t.Fatalf("result = %#v, %v", result, err)
	}
	if !reflect.DeepEqual(events, []string{"fetch", "credential", "reconcile:head"}) ||
		len(authority.acquireBases) != 0 {
		t.Fatalf("read-only events = %#v, leases = %#v", events, authority.acquireBases)
	}
}

func TestPublisherStopsOnConflictAndLostAuthority(t *testing.T) {
	for name, testCase := range map[string]struct {
		configure func(*fakeLeaseAuthority, *fakeRepository)
		want      error
	}{
		"conflict": {
			configure: func(_ *fakeLeaseAuthority, repository *fakeRepository) { repository.reconcileErr = ErrConfigConflict },
			want:      ErrConfigConflict,
		},
		"lease lost": {
			configure: func(authority *fakeLeaseAuthority, _ *fakeRepository) { authority.renewErr = ErrLeaseLost },
			want:      ErrLeaseLost,
		},
		"authorization": {
			configure: func(authority *fakeLeaseAuthority, _ *fakeRepository) { authority.credentialErr = ErrAuthorization },
			want:      ErrAuthorization,
		},
	} {
		t.Run(name, func(t *testing.T) {
			events := []string{}
			authority := &fakeLeaseAuthority{events: &events}
			repository := &fakeRepository{
				events: &events, fetches: []RemoteSnapshot{{Revision: "head"}, {Revision: "head"}, {Revision: "head"}},
				prepared: PreparedPublication{ExpectedRemoteRevision: "head", CommitID: "commit", HasChanges: true},
			}
			testCase.configure(authority, repository)
			publisher, _ := NewPublisher(PublisherConfig{Authority: authority, Repository: repository})
			if _, err := publisher.Sync(context.Background(), "head"); !errors.Is(err, testCase.want) {
				t.Fatalf("error = %v, want %v", err, testCase.want)
			}
			for _, event := range events {
				if event == "publish" {
					t.Fatalf("publication occurred after %s: %#v", name, events)
				}
			}
		})
	}
}

func TestPublisherRejectsExpiredRenewalBeforePublication(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	events := []string{}
	authority := &fakeLeaseAuthority{events: &events, expiresAt: now.Add(5 * time.Second)}
	repository := &fakeRepository{
		events: &events, fetches: []RemoteSnapshot{{Revision: "head"}, {Revision: "head"}},
		prepared: PreparedPublication{ExpectedRemoteRevision: "head", CommitID: "commit", HasChanges: true},
	}
	publisher, err := NewPublisher(PublisherConfig{
		Authority: authority, Repository: repository, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Sync(context.Background(), "head"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("error = %v, want lease lost", err)
	}
	for _, event := range events {
		if event == "publish" {
			t.Fatalf("publication occurred with no lease safety window: %#v", events)
		}
	}
}

func TestPublisherCancelsSlowPublicationBeforeLeaseExpiry(t *testing.T) {
	now := time.Now().UTC()
	events := []string{}
	authority := &fakeLeaseAuthority{events: &events, expiresAt: now.Add(1100 * time.Millisecond)}
	repository := &fakeRepository{
		events: &events, fetches: []RemoteSnapshot{{Revision: "head"}, {Revision: "head"}, {Revision: "head"}},
		prepared:     PreparedPublication{ExpectedRemoteRevision: "head", CommitID: "commit", HasChanges: true},
		publishBlock: true, observedHead: "head",
	}
	publisher, err := NewPublisher(PublisherConfig{
		Authority: authority, Repository: repository, LeaseTTL: 15 * time.Second,
		PublicationMargin: time.Second, Clock: func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := publisher.Sync(context.Background(), "head")
	if !errors.Is(err, ErrSyncUncertain) || !result.Uncertain {
		t.Fatalf("result = %#v, error = %v, want uncertain publication", result, err)
	}
	if !reflect.DeepEqual(events, []string{
		"fetch", "credential", "reconcile:head", "acquire:head", "fetch", "reconcile:head",
		"renew", "credential", "fetch", "publish", "observe", "release:head",
	}) {
		t.Fatalf("events = %#v", events)
	}
}

func TestPublisherObservesAmbiguousPushWithoutRetry(t *testing.T) {
	events := []string{}
	authority := &fakeLeaseAuthority{events: &events}
	repository := &fakeRepository{
		events: &events, fetches: []RemoteSnapshot{{Revision: "head"}, {Revision: "head"}, {Revision: "head"}},
		prepared:  PreparedPublication{ExpectedRemoteRevision: "head", CommitID: "commit", HasChanges: true},
		published: PublishResult{Uncertain: true}, publishErr: errors.New("connection reset"),
		observed: true, observedHead: "commit",
	}
	publisher, _ := NewPublisher(PublisherConfig{Authority: authority, Repository: repository})
	result, err := publisher.Sync(context.Background(), "head")
	if err != nil || !result.Landed || result.RemoteRevision != "commit" {
		t.Fatalf("result = %#v, %v", result, err)
	}
	publishes := 0
	for _, event := range events {
		if event == "publish" {
			publishes++
		}
	}
	if publishes != 1 {
		t.Fatalf("publish count = %d, events = %#v", publishes, events)
	}
}

func TestPublisherAcknowledgesBaselineOnlyAfterProvenOutcome(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		prepared   PreparedPublication
		published  PublishResult
		publishErr error
		observed   bool
		want       string
	}{
		{
			name: "no changes", prepared: PreparedPublication{
				ExpectedRemoteRevision: "head", CommitID: "head",
			}, want: "head:head",
		},
		{
			name: "direct success", prepared: PreparedPublication{
				ExpectedRemoteRevision: "head", CommitID: "commit", HasChanges: true,
			}, published: PublishResult{RemoteRevision: "commit", Landed: true}, want: "commit:commit",
		},
		{
			name: "observed uncertain success", prepared: PreparedPublication{
				ExpectedRemoteRevision: "head", CommitID: "commit", HasChanges: true,
			}, published: PublishResult{Uncertain: true}, publishErr: errors.New("connection reset"),
			observed: true, want: "commit:commit",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			events := []string{}
			authority := &fakeLeaseAuthority{events: &events}
			base := &fakeRepository{
				events: &events, fetches: []RemoteSnapshot{{Revision: "head"}, {Revision: "head"}, {Revision: "head"}},
				prepared: testCase.prepared, published: testCase.published,
				publishErr: testCase.publishErr, observed: testCase.observed, observedHead: "commit",
			}
			repository := &observingRepository{fakeRepository: base}
			publisher, _ := NewPublisher(PublisherConfig{Authority: authority, Repository: repository})
			if _, err := publisher.Sync(context.Background(), "head"); err != nil {
				t.Fatal(err)
			}
			if len(repository.committed) != 1 || repository.committed[0] != testCase.want {
				t.Fatalf("committed = %#v", repository.committed)
			}
		})
	}
}
