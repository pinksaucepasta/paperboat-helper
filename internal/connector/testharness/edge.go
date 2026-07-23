// Package testharness provides a deterministic edge for connector lifecycle tests.
package testharness

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/connector"
)

var ErrRevoked = errors.New("fake edge admission revoked")

type Edge struct {
	mu       sync.Mutex
	revoked  bool
	accepted []connector.Admission
	clients  map[*Client]struct{}
}

func New() *Edge { return &Edge{clients: make(map[*Client]struct{})} }

func (e *Edge) Factory(admission connector.Admission, _ connector.Transport) (connector.FRPClient, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.revoked {
		return nil, ErrRevoked
	}
	client := &Client{edge: e, admission: admission, ready: true, done: make(chan struct{})}
	e.accepted = append(e.accepted, admission)
	e.clients[client] = struct{}{}
	return client, nil
}

func (e *Edge) Revoke() {
	e.mu.Lock()
	e.revoked = true
	clients := make([]*Client, 0, len(e.clients))
	for client := range e.clients {
		clients = append(clients, client)
	}
	e.mu.Unlock()
	for _, client := range clients {
		client.Close()
	}
}

func (e *Edge) Accepted() []connector.Admission {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]connector.Admission(nil), e.accepted...)
}

func (e *Edge) Active() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.clients)
}

func (e *Edge) release(client *Client) {
	e.mu.Lock()
	delete(e.clients, client)
	e.mu.Unlock()
}

type Client struct {
	edge      *Edge
	admission connector.Admission
	ready     bool
	mu        sync.Mutex
	cancel    context.CancelFunc
	done      chan struct{}
	closed    bool
}

func (c *Client) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	closed := c.closed
	c.mu.Unlock()
	if closed {
		cancel()
	}
	defer func() { c.edge.release(c); close(c.done) }()
	<-runCtx.Done()
	return runCtx.Err()
}

func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Client) GracefulClose(_ time.Duration) { c.Close() }

func (c *Client) ProxyRunning(name string) bool {
	return name != "" && len(c.admission.Routes) > 0 && c.ready
}

func (c *Client) Done() <-chan struct{} { return c.done }
