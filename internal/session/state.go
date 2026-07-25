package session

import (
	"errors"
	"fmt"
	"sync"
)

type State string

const (
	Creating   State = "creating"
	Running    State = "running"
	Exited     State = "exited"
	Restarting State = "restarting"
	Closing    State = "closing"
	Closed     State = "closed"
	Deleted    State = "deleted"
)

var ErrInvalidTransition = errors.New("invalid session transition")

var transitions = map[State]map[State]bool{
	Creating:   {Running: true, Exited: true, Closed: true},
	Running:    {Exited: true, Closing: true, Restarting: true},
	Exited:     {Restarting: true, Closing: true, Deleted: true},
	Restarting: {Running: true, Exited: true, Closed: true},
	Closing:    {Closed: true},
	Closed:     {Restarting: true, Deleted: true},
	Deleted:    {},
}

type Lifecycle struct {
	mu         sync.RWMutex
	state      State
	generation uint64
}

func NewLifecycle() *Lifecycle { return &Lifecycle{state: Creating} }

func RecoverLifecycle(state State, generation uint64) (*Lifecycle, error) {
	if _, ok := transitions[state]; !ok || state == Creating && generation != 0 || (state == Running || state == Exited || state == Restarting || state == Closing) && generation == 0 {
		return nil, ErrInvalidTransition
	}
	return &Lifecycle{state: state, generation: generation}, nil
}

func (l *Lifecycle) Snapshot() (State, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state, l.generation
}

func (l *Lifecycle) Transition(to State) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !transitions[l.state][to] {
		return fmt.Errorf("%s to %s: %w", l.state, to, ErrInvalidTransition)
	}
	if to == Running && (l.state == Creating || l.state == Restarting) {
		l.generation++
	}
	l.state = to
	return nil
}

type AttachmentState string

const (
	Attaching AttachmentState = "attaching"
	Attached  AttachmentState = "attached"
	Detached  AttachmentState = "detached"
	Evicted   AttachmentState = "evicted"
)

var attachmentTransitions = map[AttachmentState]map[AttachmentState]bool{
	Attaching: {Attached: true, Detached: true, Evicted: true},
	Attached:  {Detached: true, Evicted: true},
	Detached:  {},
	Evicted:   {},
}

type Attachment struct {
	mu    sync.RWMutex
	state AttachmentState
}

func NewAttachment() *Attachment { return &Attachment{state: Attaching} }

func (a *Attachment) Transition(to AttachmentState) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !attachmentTransitions[a.state][to] {
		return fmt.Errorf("attachment %s to %s: %w", a.state, to, ErrInvalidTransition)
	}
	a.state = to
	return nil
}
