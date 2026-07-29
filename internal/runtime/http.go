package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	ErrHTTPInvalid = errors.New("invalid runtime HTTP configuration")
	ErrHTTPState   = errors.New("invalid runtime HTTP state")
)

type ListenerFactory func() (net.Listener, error)

type HTTPConfig struct {
	Address           string
	Handler           http.Handler
	Listener          ListenerFactory
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
	NativeHandler     func(net.Conn)
}

type HTTPService struct {
	mu       sync.Mutex
	config   HTTPConfig
	server   *http.Server
	listener net.Listener
	done     chan struct{}
	serveErr error
	running  bool
	stopped  bool
}

func NewHTTPService(config HTTPConfig) (*HTTPService, error) {
	if config.ReadHeaderTimeout == 0 {
		config.ReadHeaderTimeout = 5 * time.Second
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = 60 * time.Second
	}
	if config.MaxHeaderBytes == 0 {
		config.MaxHeaderBytes = 32 << 10
	}
	if config.Handler == nil || config.ReadHeaderTimeout <= 0 || config.IdleTimeout <= 0 || config.MaxHeaderBytes < 1024 || config.MaxHeaderBytes > 64<<10 || config.Listener == nil && !LoopbackAddress(config.Address) {
		return nil, ErrHTTPInvalid
	}
	return &HTTPService{config: config}, nil
}

func (s *HTTPService) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running || s.stopped {
		return ErrHTTPState
	}
	var listener net.Listener
	var err error
	if s.config.Listener != nil {
		listener, err = s.config.Listener()
	} else {
		listener, err = net.Listen("tcp", s.config.Address)
	}
	if err != nil {
		return err
	}
	if s.config.NativeHandler != nil {
		listener = &protocolListener{Listener: listener, native: s.config.NativeHandler, timeout: s.config.ReadHeaderTimeout}
	}
	server := &http.Server{Handler: s.config.Handler, ReadHeaderTimeout: s.config.ReadHeaderTimeout, IdleTimeout: s.config.IdleTimeout, MaxHeaderBytes: s.config.MaxHeaderBytes}
	s.server, s.listener, s.done, s.running = server, listener, make(chan struct{}), true
	go func() {
		err := server.Serve(listener)
		s.mu.Lock()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.serveErr = err
		}
		s.running = false
		close(s.done)
		s.mu.Unlock()
	}()
	return nil
}

type protocolListener struct {
	net.Listener
	native  func(net.Conn)
	timeout time.Duration
}

func (l *protocolListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		_ = conn.SetReadDeadline(time.Now().Add(l.timeout))
		var prefix [4]byte
		_, err = io.ReadFull(conn, prefix[:])
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			_ = conn.Close()
			continue
		}
		wrapped := &prefixedConn{Conn: conn, reader: io.MultiReader(bytes.NewReader(prefix[:]), conn)}
		if string(prefix[:]) == "PBT1" {
			go l.native(wrapped)
			continue
		}
		return wrapped, nil
	}
}

type prefixedConn struct {
	net.Conn
	reader io.Reader
}

func (c *prefixedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (s *HTTPService) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		err := s.serveErr
		s.mu.Unlock()
		return err
	}
	s.stopped = true
	server, done, running := s.server, s.done, s.running
	s.mu.Unlock()
	if server == nil {
		return nil
	}
	var shutdownErr error
	if running {
		shutdownErr = server.Shutdown(ctx)
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			_ = server.Close()
			return errors.Join(shutdownErr, ctx.Err())
		}
	}
	s.mu.Lock()
	serveErr := s.serveErr
	s.mu.Unlock()
	return errors.Join(shutdownErr, serveErr)
}

func (s *HTTPService) Address() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *HTTPService) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.serveErr }

func LoopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
