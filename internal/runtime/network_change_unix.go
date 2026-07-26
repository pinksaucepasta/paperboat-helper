//go:build darwin || linux

package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type networkChangeService struct {
	interval time.Duration
	snapshot func() (string, error)
	changed  func()
	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
}

func newNetworkChangeService(interval time.Duration, changed func()) (*networkChangeService, error) {
	if interval <= 0 || interval > time.Minute || changed == nil {
		return nil, ErrProductionInvalid
	}
	return &networkChangeService{interval: interval, snapshot: productionNetworkSnapshot, changed: changed}, nil
}

func (s *networkChangeService) Start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return ErrProductionInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go s.run(ctx, s.done)
	return nil
}

func (s *networkChangeService) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	previous, _ := s.snapshot()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current, err := s.snapshot()
			if err == nil && current != previous {
				previous = current
				s.changed()
			}
		}
	}
}

func (s *networkChangeService) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func productionNetworkSnapshot() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	values := make([]string, 0, len(interfaces)*2)
	for _, item := range interfaces {
		values = append(values, item.Name+"|"+item.HardwareAddr.String()+"|"+item.Flags.String())
		addresses, addressErr := item.Addrs()
		if addressErr != nil {
			return "", addressErr
		}
		for _, address := range addresses {
			values = append(values, item.Name+"|"+address.String())
		}
	}
	resolver, err := os.ReadFile("/etc/resolv.conf")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if len(resolver) > 64<<10 {
		return "", ErrProductionInvalid
	}
	values = append(values, "dns|"+strings.TrimSpace(string(resolver)))
	sort.Strings(values)
	digest := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return hex.EncodeToString(digest[:]), nil
}
