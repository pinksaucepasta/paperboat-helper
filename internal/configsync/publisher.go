package configsync

import (
	"context"
	"errors"
	"time"
)

var (
	ErrRemoteRevisionChanged = errors.New("config repository remote revision changed")
	ErrConfigConflict        = errors.New("configuration conflict requires explicit resolution")
	ErrSyncUncertain         = errors.New("configuration publication outcome is uncertain")
)

type LeaseAuthority interface {
	Credential(context.Context) (Credential, error)
	AcquireLease(context.Context, string, time.Duration) (Lease, error)
	RenewLease(context.Context, Lease, time.Duration) (Lease, error)
	ReleaseLease(context.Context, Lease) error
}

type RemoteSnapshot struct {
	Revision string
}

type PreparedPublication struct {
	ExpectedRemoteRevision string
	CommitID               string
	HasChanges             bool
}

type PublishResult struct {
	RemoteRevision string
	Landed         bool
	Uncertain      bool
}

type Repository interface {
	Fetch(context.Context) (RemoteSnapshot, error)
	Reconcile(context.Context, RemoteSnapshot) (PreparedPublication, error)
	Publish(context.Context, PreparedPublication, int64) (PublishResult, error)
	ObserveCommit(context.Context, string) (bool, string, error)
}

type PublicationObserver interface {
	PublicationCommitted(context.Context, PreparedPublication, string) error
}

type PublicationJournal interface {
	PublicationPrepared(context.Context, PreparedPublication) error
	PublicationAborted(context.Context, PreparedPublication) error
}

type PublisherConfig struct {
	Authority         LeaseAuthority
	Repository        Repository
	LeaseTTL          time.Duration
	ReleaseTimeout    time.Duration
	PublicationMargin time.Duration
	Clock             func() time.Time
}

type Publisher struct {
	authority         LeaseAuthority
	repository        Repository
	leaseTTL          time.Duration
	releaseTimeout    time.Duration
	publicationMargin time.Duration
	clock             func() time.Time
}

func NewPublisher(config PublisherConfig) (*Publisher, error) {
	if config.Authority == nil || config.Repository == nil {
		return nil, ErrControlClientInvalid
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = time.Minute
	}
	if config.LeaseTTL < 15*time.Second || config.LeaseTTL > 2*time.Minute {
		return nil, ErrControlClientInvalid
	}
	if config.ReleaseTimeout == 0 {
		config.ReleaseTimeout = 5 * time.Second
	}
	if config.ReleaseTimeout <= 0 || config.ReleaseTimeout > 30*time.Second {
		return nil, ErrControlClientInvalid
	}
	if config.PublicationMargin == 0 {
		config.PublicationMargin = 5 * time.Second
	}
	if config.PublicationMargin < time.Second || config.PublicationMargin >= config.LeaseTTL {
		return nil, ErrControlClientInvalid
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	return &Publisher{
		authority: config.Authority, repository: config.Repository,
		leaseTTL: config.LeaseTTL, releaseTimeout: config.ReleaseTimeout,
		publicationMargin: config.PublicationMargin, clock: config.Clock,
	}, nil
}

// Sync performs exactly one publication attempt. It never retries a push. A
// changed head is reconciled under a newly acquired lease, conflicted paths are
// omitted by the reconciler, and an ambiguous push is resolved only by remote
// observation.
func (p *Publisher) Sync(ctx context.Context, _ string) (result PublishResult, resultErr error) {
	remote, err := p.repository.Fetch(ctx)
	if err != nil {
		return PublishResult{}, err
	}
	if remote.Revision == "" {
		return PublishResult{}, ErrRemoteRevisionChanged
	}
	// Pull and conflict observation do not require a writer lease. Revalidate
	// current authorization immediately before the reconciler may mutate the
	// managed filesystem.
	if _, err := p.authority.Credential(ctx); err != nil {
		return PublishResult{}, errors.Join(ErrAuthorization, err)
	}
	prepared, err := p.repository.Reconcile(ctx, remote)
	if errors.Is(err, ErrConfigConflict) {
		return PublishResult{RemoteRevision: remote.Revision}, ErrConfigConflict
	}
	if err != nil {
		return PublishResult{}, err
	}
	if !prepared.HasChanges {
		if observer, ok := p.repository.(PublicationObserver); ok {
			if observeErr := observer.PublicationCommitted(ctx, prepared, remote.Revision); observeErr != nil {
				return PublishResult{}, observeErr
			}
		}
		return PublishResult{RemoteRevision: remote.Revision, Landed: true}, nil
	}

	lease, err := p.authority.AcquireLease(ctx, remote.Revision, p.leaseTTL)
	if err != nil {
		return PublishResult{}, err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.releaseTimeout)
		defer cancel()
		releaseErr := p.authority.ReleaseLease(releaseCtx, lease)
		if resultErr == nil {
			resultErr = releaseErr
		}
	}()
	verified, err := p.repository.Fetch(ctx)
	if err != nil {
		return PublishResult{}, err
	}
	if verified.Revision != remote.Revision || lease.BaseRevision != remote.Revision {
		return PublishResult{}, ErrRemoteRevisionChanged
	}
	// Every publication is prepared again under its lease so the pushed commit
	// is derived from the exact leased head.
	prepared, err = p.repository.Reconcile(ctx, verified)
	if err != nil {
		return PublishResult{}, err
	}
	if !prepared.HasChanges {
		if observer, ok := p.repository.(PublicationObserver); ok {
			if observeErr := observer.PublicationCommitted(ctx, prepared, verified.Revision); observeErr != nil {
				return PublishResult{}, observeErr
			}
		}
		return PublishResult{RemoteRevision: verified.Revision, Landed: true}, nil
	}
	if prepared.ExpectedRemoteRevision != remote.Revision || prepared.CommitID == "" {
		return PublishResult{}, ErrRemoteRevisionChanged
	}

	lease, err = p.authority.RenewLease(ctx, lease, p.leaseTTL)
	if err != nil {
		return PublishResult{}, err
	}
	publicationDeadline := lease.ExpiresAt.Add(-p.publicationMargin)
	if !publicationDeadline.After(p.clock().UTC()) {
		return PublishResult{}, ErrLeaseLost
	}
	publicationCtx, cancelPublication := context.WithDeadline(ctx, publicationDeadline)
	defer cancelPublication()
	if _, err := p.authority.Credential(publicationCtx); err != nil {
		return PublishResult{}, errors.Join(ErrAuthorization, err)
	}
	current, err := p.repository.Fetch(publicationCtx)
	if err != nil {
		return PublishResult{}, err
	}
	if current.Revision != prepared.ExpectedRemoteRevision {
		return PublishResult{}, ErrRemoteRevisionChanged
	}

	if journal, ok := p.repository.(PublicationJournal); ok {
		if err := journal.PublicationPrepared(publicationCtx, prepared); err != nil {
			return PublishResult{}, err
		}
	}
	published, err := p.repository.Publish(publicationCtx, prepared, lease.FencingToken)
	if err == nil && published.Landed && !published.Uncertain && published.RemoteRevision != "" {
		if observer, ok := p.repository.(PublicationObserver); ok {
			if observeErr := observer.PublicationCommitted(ctx, prepared, published.RemoteRevision); observeErr != nil {
				return PublishResult{}, observeErr
			}
		}
		return published, nil
	}
	if err != nil && !published.Uncertain {
		if journal, ok := p.repository.(PublicationJournal); ok {
			if abortErr := journal.PublicationAborted(ctx, prepared); abortErr != nil {
				return PublishResult{}, errors.Join(err, abortErr)
			}
		}
		return PublishResult{}, err
	}
	landed, revision, observeErr := p.repository.ObserveCommit(ctx, prepared.CommitID)
	if observeErr != nil {
		return PublishResult{Uncertain: true}, errors.Join(ErrSyncUncertain, observeErr)
	}
	if landed {
		if observer, ok := p.repository.(PublicationObserver); ok {
			if commitErr := observer.PublicationCommitted(ctx, prepared, revision); commitErr != nil {
				return PublishResult{}, commitErr
			}
		}
		return PublishResult{RemoteRevision: revision, Landed: true}, nil
	}
	return PublishResult{RemoteRevision: revision, Uncertain: true}, ErrSyncUncertain
}
