package filetransfer

import (
	"errors"
	"sync"
	"time"
)

var ErrNoActiveWriter = errors.New("no_active_writer")

type writerAttachment struct {
	clientID  string
	lastInput time.Time
}
type WriterRegistry struct {
	mu       sync.Mutex
	sessions map[string]map[string]writerAttachment
}

func NewWriterRegistry() *WriterRegistry {
	return &WriterRegistry{sessions: make(map[string]map[string]writerAttachment)}
}
func (r *WriterRegistry) Attach(sessionID, attachmentID, clientID string) {
	if sessionID == "" || attachmentID == "" || clientID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attachments := r.sessions[sessionID]
	if attachments == nil {
		attachments = make(map[string]writerAttachment)
		r.sessions[sessionID] = attachments
	}
	attachments[attachmentID] = writerAttachment{clientID: clientID}
}
func (r *WriterRegistry) Input(sessionID, attachmentID, clientID string, at time.Time) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attachments := r.sessions[sessionID]
	current, ok := attachments[attachmentID]
	if !ok || current.clientID != clientID {
		return
	}
	current.lastInput = at
	attachments[attachmentID] = current
}
func (r *WriterRegistry) Detach(sessionID, attachmentID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	attachments := r.sessions[sessionID]
	delete(attachments, attachmentID)
	if len(attachments) == 0 {
		delete(r.sessions, sessionID)
	}
}
func (r *WriterRegistry) Recipient(sessionID string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	attachments := r.sessions[sessionID]
	clients := make(map[string]time.Time)
	for _, attachment := range attachments {
		if attachment.lastInput.After(clients[attachment.clientID]) {
			clients[attachment.clientID] = attachment.lastInput
		} else if _, ok := clients[attachment.clientID]; !ok {
			clients[attachment.clientID] = time.Time{}
		}
	}
	var selected string
	var latest time.Time
	for client, at := range clients {
		if !at.IsZero() && (selected == "" || at.After(latest) || at.Equal(latest) && client < selected) {
			selected, latest = client, at
		}
	}
	if selected != "" {
		return selected, nil
	}
	if len(clients) == 1 {
		for client := range clients {
			return client, nil
		}
	}
	return "", ErrNoActiveWriter
}
