package runtime

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/health"
)

type oneConnectionListener struct {
	mu         sync.Mutex
	connection net.Conn
	closed     chan struct{}
	serveErr   error
}

func (l *oneConnectionListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.connection != nil {
		connection := l.connection
		l.connection = nil
		l.mu.Unlock()
		return connection, nil
	}
	l.mu.Unlock()
	<-l.closed
	if l.serveErr != nil {
		return nil, l.serveErr
	}
	return nil, net.ErrClosed
}
func (l *oneConnectionListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (*oneConnectionListener) Addr() net.Addr { return runtimeAddr("pipe") }

type runtimeAddr string

func (a runtimeAddr) Network() string { return "pipe" }
func (a runtimeAddr) String() string  { return string(a) }

func TestHTTPServiceRejectsNonLoopbackProductionAddress(t *testing.T) {
	for _, address := range []string{"", ":8080", "0.0.0.0:8080", "192.0.2.1:8080", "localhost:8080"} {
		if _, err := NewHTTPService(HTTPConfig{Address: address, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}); !errors.Is(err, ErrHTTPInvalid) {
			t.Fatalf("address=%q err=%v", address, err)
		}
	}
	if _, err := NewHTTPService(HTTPConfig{Address: "127.0.0.1:8080", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPServiceServesInjectedListenerAndShutsDown(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	listener := &oneConnectionListener{connection: serverSide, closed: make(chan struct{})}
	service, err := NewHTTPService(HTTPConfig{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(writer, "ok")
	}), Listener: func() (net.Listener, error) { return listener, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if service.Address() == nil {
		t.Fatal("listener address unavailable")
	}
	if _, err := io.WriteString(clientSide, "GET /health HTTP/1.1\r\nHost: helper\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(clientSide), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	clientSide.Close()
	if response.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); !errors.Is(err, ErrHTTPState) {
		t.Fatalf("restart err=%v", err)
	}
}

func TestHTTPServiceReportsUnexpectedServeFailure(t *testing.T) {
	failure := errors.New("listener failed")
	listener := &oneConnectionListener{closed: make(chan struct{}), serveErr: failure}
	close(listener.closed)
	service, err := NewHTTPService(HTTPConfig{Handler: http.NotFoundHandler(), Listener: func() (net.Listener, error) { return listener, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for service.Err() == nil {
		select {
		case <-deadline.C:
			t.Fatal("serve failure not reported")
		case <-ticker.C:
		}
	}
	if err := service.Shutdown(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("shutdown err=%v", err)
	}
}

func TestRuntimeOrdersHTTPAdmissionBeforeConnectionServerShutdown(t *testing.T) {
	order := make([]string, 0, 4)
	connectionServer := &orderedService{name: "connection_server", order: &order}
	httpAdmission := &orderedService{name: "http_admission", order: &order}
	runtime, err := NewRuntime(Config{Version: "test", Clock: health.RealClock{}, Components: []Component{
		{Capability: "terminal.v2", Required: true, Service: connectionServer},
		{Capability: "health.v1", Required: true, Service: httpAdmission},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:connection_server", "start:http_admission", "stop:http_admission", "stop:connection_server"}
	if len(order) != len(want) {
		t.Fatalf("order=%v", order)
	}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("order=%v", order)
		}
	}
}

type orderedService struct {
	name  string
	order *[]string
}

func (s *orderedService) Start(context.Context) error {
	*s.order = append(*s.order, "start:"+s.name)
	return nil
}
func (s *orderedService) Shutdown(context.Context) error {
	*s.order = append(*s.order, "stop:"+s.name)
	return nil
}
