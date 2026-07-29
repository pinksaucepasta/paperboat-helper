package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	frpclient "github.com/fatedier/frp/client"
	v1 "github.com/fatedier/frp/pkg/config/v1"
)

type transportPreference struct {
	Transport Transport `json:"transport"`
	ExpiresAt time.Time `json:"expires_at"`
}

type racingConnector struct {
	ctx            context.Context
	cfg            *v1.ClientCommonConfig
	preferencePath string
	mu             sync.Mutex
	winner         frpclient.Connector
	raceCancel     context.CancelFunc
	closed         bool
	newConnector   func(context.Context, *v1.ClientCommonConfig) frpclient.Connector
	onSelected     func(Transport)
}

func newRacingConnector(ctx context.Context, cfg *v1.ClientCommonConfig, preferencePath string) frpclient.Connector {
	return &racingConnector{ctx: ctx, cfg: cfg, preferencePath: preferencePath, newConnector: frpclient.NewConnector}
}

type connectorOpenResult struct {
	transport Transport
	connector frpclient.Connector
	cancel    context.CancelFunc
	err       error
}

func (c *racingConnector) Open() error {
	raceCtx, raceCancel := context.WithCancel(c.ctx)
	c.mu.Lock()
	if c.closed || c.winner != nil || c.raceCancel != nil {
		c.mu.Unlock()
		raceCancel()
		return ErrUnavailable
	}
	c.raceCancel = raceCancel
	c.mu.Unlock()
	first, second := QUIC, TCPMux
	if preferred := c.loadPreference(); preferred == TCPMux {
		first, second = second, first
	}
	results := make(chan connectorOpenResult, 2)
	candidates := make([]connectorOpenResult, 0, 2)
	start := func(transport Transport) {
		candidateCtx, cancel := context.WithCancel(raceCtx)
		cfg := *c.cfg
		cfg.Transport = c.cfg.Transport
		if transport == QUIC {
			cfg.Transport.Protocol = "quic"
		} else {
			cfg.Transport.Protocol = "tcp"
			enabled := true
			cfg.Transport.TCPMux = &enabled
		}
		candidate := c.newConnector(candidateCtx, &cfg)
		candidates = append(candidates, connectorOpenResult{transport: transport, connector: candidate, cancel: cancel})
		go func() {
			results <- connectorOpenResult{transport: transport, connector: candidate, cancel: cancel, err: candidate.Open()}
		}()
	}
	start(first)
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	startedSecond := false
	var failures error
	for completed := 0; completed < 2; {
		select {
		case <-timer.C:
			if !startedSecond {
				start(second)
				startedSecond = true
			}
		case result := <-results:
			completed++
			if result.err == nil {
				c.mu.Lock()
				if c.closed {
					c.mu.Unlock()
					result.cancel()
					_ = result.connector.Close()
					return ErrUnavailable
				}
				c.winner = result.connector
				c.mu.Unlock()
				if c.onSelected != nil {
					c.onSelected(result.transport)
				}
				c.savePreference(result.transport)
				for _, candidate := range candidates {
					if candidate.connector != result.connector {
						candidate.cancel()
						_ = candidate.connector.Close()
					}
				}
				return nil
			}
			result.cancel()
			_ = result.connector.Close()
			failures = errors.Join(failures, result.err)
			if !startedSecond {
				if timer.Stop() {
				}
				start(second)
				startedSecond = true
			}
		case <-raceCtx.Done():
			raceCancel()
			for _, candidate := range candidates {
				candidate.cancel()
				_ = candidate.connector.Close()
			}
			if err := c.ctx.Err(); err != nil {
				return err
			}
			return ErrUnavailable
		}
	}
	raceCancel()
	c.mu.Lock()
	c.raceCancel = nil
	c.mu.Unlock()
	return errors.Join(ErrUnavailable, failures)
}

func (c *racingConnector) Connect() (net.Conn, error) {
	c.mu.Lock()
	winner := c.winner
	closed := c.closed
	c.mu.Unlock()
	if winner == nil || closed {
		return nil, ErrUnavailable
	}
	return winner.Connect()
}
func (c *racingConnector) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	winner := c.winner
	raceCancel := c.raceCancel
	c.mu.Unlock()
	if raceCancel != nil {
		raceCancel()
	}
	if winner != nil {
		return winner.Close()
	}
	return nil
}

func (c *racingConnector) loadPreference() Transport {
	data, err := os.ReadFile(c.preferencePath)
	if err != nil {
		return ""
	}
	var value transportPreference
	if json.Unmarshal(data, &value) != nil || !time.Now().Before(value.ExpiresAt) {
		return ""
	}
	return value.Transport
}
func (c *racingConnector) savePreference(transport Transport) {
	if c.preferencePath == "" {
		return
	}
	data, _ := json.Marshal(transportPreference{Transport: transport, ExpiresAt: time.Now().Add(30 * time.Minute)})
	dir := filepath.Dir(c.preferencePath)
	if os.MkdirAll(dir, 0700) != nil {
		return
	}
	file, err := os.CreateTemp(dir, "connector-transport-*")
	if err != nil {
		return
	}
	name := file.Name()
	defer os.Remove(name)
	_ = file.Chmod(0600)
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		_ = os.Rename(name, c.preferencePath)
	}
}
