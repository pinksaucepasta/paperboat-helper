package operation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

var (
	ErrInvalidOperationID = errors.New("invalid operation id")
	ErrInvalidRequest     = errors.New("invalid canonical request")
	ErrOperationConflict  = errors.New("operation id conflict")
	ErrJournalFull        = errors.New("operation journal is full")
	ErrOperationUncertain = errors.New("operation outcome is uncertain")
)

type Outcome struct {
	Result    json.RawMessage
	ErrorCode string
}

type entry struct {
	hash     [sha256.Size]byte
	done     chan struct{}
	outcome  Outcome
	complete bool
	err      error
}

type Journal struct {
	mu          sync.Mutex
	max         int
	entries     map[string]*entry
	order       []string
	store       *store.Store
	retention   time.Duration
	now         func() time.Time
	lastCleanup time.Time
}

const cleanupInterval = time.Minute

func NewPersistentJournal(ctx context.Context, maxEntries int, durable *store.Store, retention time.Duration, now func() time.Time) (*Journal, error) {
	if durable == nil || retention <= 0 {
		return nil, ErrJournalFull
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	journal, err := NewJournal(maxEntries)
	if err != nil {
		return nil, err
	}
	journal.store, journal.retention, journal.now = durable, retention, now
	currentTime := now()
	records, err := durable.Operations(ctx, currentTime, maxEntries)
	if err != nil {
		return nil, err
	}
	journal.lastCleanup = currentTime
	for _, record := range records {
		if len(record.RequestHash) != sha256.Size {
			return nil, ErrInvalidRequest
		}
		var hash [sha256.Size]byte
		copy(hash[:], record.RequestHash)
		entry := &entry{hash: hash, done: make(chan struct{}), complete: true}
		if record.State == "pending" {
			entry.err = ErrOperationUncertain
		} else if record.State == "completed" {
			entry.outcome = Outcome{Result: append(json.RawMessage(nil), record.Result...), ErrorCode: record.ErrorCode}
			journal.order = append(journal.order, record.OperationID)
		} else {
			return nil, ErrInvalidRequest
		}
		close(entry.done)
		journal.entries[record.OperationID] = entry
	}
	return journal, nil
}

func NewJournal(maxEntries int) (*Journal, error) {
	if maxEntries < 1 {
		return nil, ErrJournalFull
	}
	return &Journal{max: maxEntries, entries: make(map[string]*entry)}, nil
}

func CanonicalHash(request []byte) ([sha256.Size]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(request))
	dec.UseNumber()
	value, err := decodeValue(dec)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return [sha256.Size]byte{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return sha256.Sum256(canonical), nil
}

func (j *Journal) Execute(ctx context.Context, operationID string, request []byte, run func(context.Context) Outcome) (Outcome, bool, error) {
	if len(operationID) < 8 || len(operationID) > 128 || run == nil {
		return Outcome{}, false, ErrInvalidOperationID
	}
	hash, err := CanonicalHash(request)
	if err != nil {
		return Outcome{}, false, err
	}
	j.mu.Lock()
	if existing, ok := j.entries[operationID]; ok {
		if existing.hash != hash {
			j.mu.Unlock()
			return Outcome{}, false, ErrOperationConflict
		}
		j.mu.Unlock()
		select {
		case <-existing.done:
			return cloneOutcome(existing.outcome), true, existing.err
		case <-ctx.Done():
			return Outcome{}, false, ctx.Err()
		}
	}
	for len(j.entries) >= j.max && len(j.order) > 0 {
		oldest := j.order[0]
		j.order = j.order[1:]
		delete(j.entries, oldest)
	}
	if len(j.entries) >= j.max {
		j.mu.Unlock()
		return Outcome{}, false, ErrJournalFull
	}
	if j.store != nil {
		now := j.now()
		if !now.Before(j.lastCleanup.Add(cleanupInterval)) {
			if cleanupErr := j.store.DeleteExpiredOperations(ctx, now); cleanupErr != nil {
				j.mu.Unlock()
				return Outcome{}, false, cleanupErr
			}
			j.lastCleanup = now
		}
		record, inserted, reserveErr := j.store.ReserveOperation(ctx, operationID, hash[:], j.now().Add(j.retention))
		if reserveErr != nil {
			j.mu.Unlock()
			if errors.Is(reserveErr, store.ErrConflict) {
				return Outcome{}, false, ErrOperationConflict
			}
			return Outcome{}, false, reserveErr
		}
		if !inserted {
			if record.State == "pending" {
				j.mu.Unlock()
				return Outcome{}, true, ErrOperationUncertain
			}
			outcome := Outcome{Result: append(json.RawMessage(nil), record.Result...), ErrorCode: record.ErrorCode}
			j.mu.Unlock()
			return outcome, true, nil
		}
	}
	current := &entry{hash: hash, done: make(chan struct{})}
	j.entries[operationID] = current
	j.mu.Unlock()

	outcome := cloneOutcome(run(ctx))
	var persistenceErr error
	if j.store != nil {
		now := j.now()
		if len(outcome.Result) > store.MaxOperationResultBytes {
			persistenceErr = store.ErrResultTooLarge
		} else {
			persistenceErr = j.store.CompleteOperation(context.Background(), store.OperationResult{OperationID: operationID, RequestHash: hash[:], State: "completed", Result: outcome.Result, ErrorCode: outcome.ErrorCode, CompletedAt: now, ExpiresAt: now.Add(j.retention)})
		}
	}
	j.mu.Lock()
	current.outcome = outcome
	current.complete = true
	if persistenceErr != nil {
		current.err = errors.Join(ErrOperationUncertain, persistenceErr)
	} else {
		j.order = append(j.order, operationID)
	}
	close(current.done)
	j.mu.Unlock()
	return cloneOutcome(outcome), false, current.err
}

func decodeValue(dec *json.Decoder) (any, error) {
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("object key is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate object key %q", key)
			}
			value, err := decodeValue(dec)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if token, err := dec.Token(); err != nil || token != json.Delim('}') {
			return nil, errors.New("unterminated object")
		}
		return object, nil
	case '[':
		var array []any
		for dec.More() {
			value, err := decodeValue(dec)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if token, err := dec.Token(); err != nil || token != json.Delim(']') {
			return nil, errors.New("unterminated array")
		}
		return array, nil
	default:
		return nil, errors.New("unexpected delimiter")
	}
}

func cloneOutcome(outcome Outcome) Outcome {
	outcome.Result = append(json.RawMessage(nil), outcome.Result...)
	return outcome
}
