package server

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/nativeproto"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

var ErrNativeAssociation = errors.New("invalid native terminal association")

type NativeAssociationConfig struct {
	Server     *Server
	Authorizer AuthorizerFactory
	Limiter    *ConnectionLimiter
	Expiry     time.Duration
	Random     io.Reader
}

type NativeAssociationManager struct {
	config NativeAssociationConfig
	mu     sync.Mutex
	sets   map[[nativeproto.ConnectionIDSize]byte]*nativeConnection
}

func NewNativeAssociationManager(config NativeAssociationConfig) (*NativeAssociationManager, error) {
	if config.Expiry == 0 {
		config.Expiry = 10 * time.Second
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Server == nil || config.Authorizer == nil || config.Limiter == nil || config.Expiry <= 0 || config.Expiry > time.Minute {
		return nil, ErrInvalidConfiguration
	}
	return &NativeAssociationManager{config: config, sets: make(map[[nativeproto.ConnectionIDSize]byte]*nativeConnection)}, nil
}

func (m *NativeAssociationManager) Serve(conn net.Conn) error {
	if conn == nil || !m.config.Limiter.acquire() {
		if conn != nil {
			_ = conn.Close()
		}
		return ErrNativeAssociation
	}
	defer m.config.Limiter.release()
	_ = conn.SetReadDeadline(time.Now().Add(m.config.Expiry))
	preface, err := nativeproto.ReadPreface(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	_ = conn.SetReadDeadline(time.Time{})
	if preface.Role == nativeproto.RoleControl {
		return m.serveControl(conn, preface)
	}
	return m.serveAuxiliary(conn, preface)
}

func (m *NativeAssociationManager) serveControl(conn net.Conn, p nativeproto.Preface) error {
	authorizer, err := m.config.Authorizer(p.Token)
	if err != nil || authorizer == nil {
		_ = conn.Close()
		return ErrNativeAssociation
	}
	native := newNativeConnection(conn, p.ConnectionID, m.config.Expiry)
	if _, err := io.ReadFull(m.config.Random, native.binding[:]); err != nil {
		_ = conn.Close()
		return err
	}
	m.mu.Lock()
	if _, exists := m.sets[p.ConnectionID]; exists {
		m.mu.Unlock()
		_ = conn.Close()
		return ErrNativeAssociation
	}
	m.sets[p.ConnectionID] = native
	m.mu.Unlock()
	// The control bearer credential has already been validated by Authorizer.
	// Mark the association before publishing the binding secret so auxiliary
	// streams arriving immediately after welcome cannot race later requests.
	native.markAuthenticated()
	native.mu.Lock()
	native.timer = time.AfterFunc(m.config.Expiry, native.expireIncomplete)
	native.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.sets, p.ConnectionID)
		m.mu.Unlock()
		native.Close()
	}()
	return m.config.Server.ServeAuthenticated(native, authorizer)
}

func (m *NativeAssociationManager) serveAuxiliary(conn net.Conn, p nativeproto.Preface) error {
	m.mu.Lock()
	native := m.sets[p.ConnectionID]
	m.mu.Unlock()
	if native == nil || subtle.ConstantTimeCompare(p.Binding, native.binding[:]) != 1 || !native.attach(p.Role, conn) {
		_ = conn.Close()
		return ErrNativeAssociation
	}
	<-native.done
	return nil
}

type nativeConnection struct {
	control net.Conn
	id      [nativeproto.ConnectionIDSize]byte
	binding [nativeproto.BindingSize]byte
	expiry  time.Duration
	timer   *time.Timer

	mu            sync.Mutex
	input         net.Conn
	output        net.Conn
	authenticated bool
	ready         chan struct{}
	readyOnce     sync.Once
	done          chan struct{}
	closeOnce     sync.Once
	readOnce      sync.Once
	reads         chan nativeRead
	revoked       atomic.Bool
}

type nativeRead struct {
	frame  protocol.Frame
	binary []byte
	err    error
}

func newNativeConnection(control net.Conn, id [nativeproto.ConnectionIDSize]byte, expiry time.Duration) *nativeConnection {
	return &nativeConnection{control: control, id: id, expiry: expiry, ready: make(chan struct{}), done: make(chan struct{}), reads: make(chan nativeRead, 2)}
}

func (c *nativeConnection) AugmentWelcome(fields map[string]any) {
	fields["binding_secret"] = c.binding[:]
}
func (c *nativeConnection) RevocationFlag() *atomic.Bool { return &c.revoked }
func (c *nativeConnection) Read([]byte) (int, error) {
	return 0, errors.New("native connection requires application framing")
}

func (c *nativeConnection) attach(role byte, conn net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.authenticated {
		return false
	}
	if role == nativeproto.RoleInput && c.input == nil {
		c.input = conn
	} else if role == nativeproto.RoleOutput && c.output == nil {
		c.output = conn
	} else {
		return false
	}
	c.signalReadyLocked()
	return true
}

func (c *nativeConnection) markAuthenticated() {
	c.mu.Lock()
	c.authenticated = true
	c.signalReadyLocked()
	c.mu.Unlock()
}

func (c *nativeConnection) signalReadyLocked() {
	if c.authenticated && c.input != nil && c.output != nil {
		c.readyOnce.Do(func() {
			if c.timer != nil {
				c.timer.Stop()
			}
			close(c.ready)
		})
	}
}

func (c *nativeConnection) expireIncomplete() {
	select {
	case <-c.ready:
		return
	default:
		_ = c.Close()
	}
}

func (c *nativeConnection) startReaders() {
	c.readOnce.Do(func() {
		go c.readControl()
		go func() {
			select {
			case <-c.ready:
			case <-c.done:
				return
			}
			c.mu.Lock()
			input := c.input
			c.mu.Unlock()
			for {
				_, payload, err := nativeproto.ReadRecord(input, false)
				if !c.deliver(nativeRead{binary: payload, err: err}) || err != nil {
					return
				}
			}
		}()
	})
}

func (c *nativeConnection) readControl() {
	for {
		kind, payload, err := nativeproto.ReadRecord(c.control, true)
		result := nativeRead{err: err}
		if err == nil && kind == nativeproto.RecordStructured {
			err = json.Unmarshal(payload, &result.frame)
			if err == nil {
				err = result.frame.Validate()
			}
			result.err = err
		} else if err == nil {
			result.binary = payload
		}
		if !c.deliver(result) || err != nil {
			return
		}
	}
}

func (c *nativeConnection) deliver(result nativeRead) bool {
	select {
	case c.reads <- result:
		return true
	case <-c.done:
		return false
	}
}

func (c *nativeConnection) ReadApplication() (protocol.Frame, []byte, error) {
	c.startReaders()
	select {
	case result := <-c.reads:
		return result.frame, result.binary, result.err
	case <-c.done:
		return protocol.Frame{}, nil, io.EOF
	}
}

func (c *nativeConnection) Write(data []byte) (int, error) {
	if err := nativeproto.WriteRecord(c.control, nativeproto.RecordStructured, data, true); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (c *nativeConnection) WriteStructured(frame protocol.Frame) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return nativeproto.WriteRecord(c.control, nativeproto.RecordStructured, data, true)
}

func (c *nativeConnection) WriteBinary(frame protocol.BinaryFrame) error {
	data, err := protocol.EncodeTerminalOutputAdaptive(protocol.TerminalOutputFrame{Channel: frame.Channel, StreamID: 1, StartSequence: frame.StartSequence, Data: frame.Data}, nil)
	if err != nil {
		return err
	}
	return c.writeOutput(data)
}

func (c *nativeConnection) WriteTerminalOutput(streamID uint32, frame protocol.BinaryFrame) error {
	data, err := protocol.EncodeTerminalOutputAdaptive(protocol.TerminalOutputFrame{Channel: frame.Channel, StreamID: streamID, StartSequence: frame.StartSequence, Data: frame.Data}, nil)
	if err != nil {
		return err
	}
	return c.writeOutput(data)
}

func (c *nativeConnection) writeOutput(data []byte) error {
	select {
	case <-c.ready:
	case <-c.done:
		return io.ErrClosedPipe
	}
	c.mu.Lock()
	output := c.output
	c.mu.Unlock()
	return nativeproto.WriteRecord(output, nativeproto.RecordBinary, data, false)
}

func (c *nativeConnection) CloseProtocol(int, string) error { return c.Close() }
func (c *nativeConnection) Close() error {
	c.closeOnce.Do(func() {
		c.revoked.Store(true)
		close(c.done)
		_ = c.control.Close()
		c.mu.Lock()
		if c.timer != nil {
			c.timer.Stop()
		}
		if c.input != nil {
			_ = c.input.Close()
		}
		if c.output != nil {
			_ = c.output.Close()
		}
		c.mu.Unlock()
	})
	return nil
}
