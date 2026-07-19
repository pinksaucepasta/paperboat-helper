package session

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
)

type InputStatus string

const (
	InputAccepted  InputStatus = "accepted"
	InputDuplicate InputStatus = "duplicate"
	InputRejected  InputStatus = "rejected"
	InputUncertain InputStatus = "uncertain"
)

var (
	ErrInvalidInput     = errors.New("invalid input identity")
	ErrInputConflict    = errors.New("input id reused with different content")
	ErrStaleGeneration  = errors.New("stale process generation")
	ErrInputUnknown     = errors.New("input decision not found")
	ErrInputJournalFull = errors.New("input decision journal is full")
)

type StaleGenerationError struct{ CurrentGeneration uint64 }

func (e *StaleGenerationError) Error() string {
	return fmt.Sprintf("current generation %d: %v", e.CurrentGeneration, ErrStaleGeneration)
}
func (e *StaleGenerationError) Unwrap() error { return ErrStaleGeneration }

type InputKey struct {
	ClientID     string
	AttachmentID string
	Generation   uint64
	InputID      string
}

type InputDecision struct {
	Status       InputStatus `json:"status"`
	BytesWritten int         `json:"bytes_written"`
	WriteError   string      `json:"write_error,omitempty"`
	hash         [sha256.Size]byte
}

type InputWriter interface{ Write([]byte) (int, error) }

type InputJournal struct {
	mu        sync.Mutex
	current   uint64
	max       int
	decisions map[InputKey]InputDecision
}

func NewInputJournal(generation uint64) *InputJournal {
	return NewBoundedInputJournal(generation, helperconfig.DefaultResources.MaxInputDecisions)
}

func NewBoundedInputJournal(generation uint64, max int) *InputJournal {
	return &InputJournal{current: generation, max: max, decisions: make(map[InputKey]InputDecision)}
}

func (j *InputJournal) SetGeneration(generation uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.current = generation
}

func (j *InputJournal) Write(key InputKey, data []byte, writer InputWriter) (InputDecision, error) {
	if key.ClientID == "" || key.AttachmentID == "" || key.InputID == "" || key.Generation == 0 || writer == nil {
		return InputDecision{}, ErrInvalidInput
	}
	hash := sha256.Sum256(data)
	j.mu.Lock()
	defer j.mu.Unlock()
	if previous, ok := j.decisions[key]; ok {
		if previous.hash != hash {
			return InputDecision{}, ErrInputConflict
		}
		if previous.Status == InputAccepted {
			previous.Status = InputDuplicate
		}
		return publicDecision(previous), nil
	}
	if len(j.decisions) >= j.max {
		return InputDecision{}, ErrInputJournalFull
	}
	if key.Generation != j.current {
		return InputDecision{}, &StaleGenerationError{CurrentGeneration: j.current}
	}
	n, err := writer.Write(data)
	decision := InputDecision{BytesWritten: n, hash: hash}
	switch {
	case n == len(data):
		decision.Status = InputAccepted
	case n == 0:
		decision.Status = InputRejected
	default:
		decision.Status = InputUncertain
	}
	if err != nil {
		decision.WriteError = "pty_write_failed"
		if n > 0 && n < len(data) {
			decision.Status = InputUncertain
		}
	}
	j.decisions[key] = decision
	return publicDecision(decision), nil
}

func (j *InputJournal) Query(key InputKey) (InputDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	decision, ok := j.decisions[key]
	if !ok {
		return InputDecision{}, ErrInputUnknown
	}
	return publicDecision(decision), nil
}

func (j *InputJournal) Admit(key InputKey) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, exists := j.decisions[key]; exists {
		return nil
	}
	if len(j.decisions) >= j.max {
		return ErrInputJournalFull
	}
	return nil
}

func (j *InputJournal) Restore(key InputKey, hash []byte, status InputStatus, bytesWritten int, errorCode string) error {
	if len(hash) != sha256.Size || key.ClientID == "" || key.AttachmentID == "" || key.InputID == "" || key.Generation == 0 {
		return ErrInvalidInput
	}
	if status != InputAccepted && status != InputRejected && status != InputUncertain {
		return ErrInvalidInput
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash)
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, exists := j.decisions[key]; exists {
		return ErrInputConflict
	}
	if len(j.decisions) >= j.max {
		return ErrInputJournalFull
	}
	j.decisions[key] = InputDecision{Status: status, BytesWritten: bytesWritten, WriteError: errorCode, hash: digest}
	return nil
}

func publicDecision(decision InputDecision) InputDecision {
	decision.hash = [sha256.Size]byte{}
	return decision
}
