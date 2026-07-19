package preview

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func readyProxy(t *testing.T, transport http.RoundTripper) (*Proxy, string) {
	t.Helper()
	registry := newRegistry(t, &fakeProber{}, 1)
	if _, err := registry.Register("prv", "env", "web", Target{Host: "127.0.0.1", Port: 23456}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Probe(context.Background(), "prv"); err != nil {
		t.Fatal(err)
	}
	proxy, err := NewProxy(ProxyConfig{Registry: registry, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	return proxy, "prv"
}

type pipeListener struct {
	mu     sync.Mutex
	conn   net.Conn
	closed chan struct{}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.conn != nil {
		connection := l.conn
		l.conn = nil
		l.mu.Unlock()
		return connection, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, net.ErrClosed
}
func (l *pipeListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (*pipeListener) Addr() net.Addr { return pipeAddress("preview") }

type pipeAddress string

func (a pipeAddress) Network() string { return "pipe" }
func (a pipeAddress) String() string  { return string(a) }

func servePipe(t *testing.T, handler http.Handler) (net.Conn, func()) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	listener := &pipeListener{conn: serverSide, closed: make(chan struct{})}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	done := make(chan struct{})
	go func() { _ = server.Serve(listener); close(done) }()
	return clientSide, func() {
		_ = clientSide.Close()
		_ = listener.Close()
		<-done
	}
}

func pipeTransport(connection net.Conn) *http.Transport {
	used := false
	return &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		if used {
			return nil, net.ErrClosed
		}
		used = true
		return connection, nil
	}}
}

func TestProxyForwardsHTTPAndSanitizesHeaders(t *testing.T) {
	var expectedHost string
	targetConnection, closeTarget := servePipe(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Forwarded-For") != "" || request.Header.Get("Forwarded") != "" || request.Header.Get("X-Paperboat-Preview-ID") != "" || request.Host != expectedHost {
			t.Errorf("forwarded headers=%v host=%q url=%q", request.Header, request.Host, request.URL.Host)
		}
		writer.Header().Set("Content-Type", "text/plain")
		_, _ = writer.Write([]byte("ok:" + request.URL.Path))
	}))
	defer closeTarget()
	expectedHost = "127.0.0.1:23456"
	targetTransport := pipeTransport(targetConnection)
	defer targetTransport.CloseIdleConnections()
	proxy, identity := readyProxy(t, targetTransport)
	request := httptest.NewRequest(http.MethodGet, "http://preview.test/hello", nil)
	request.Header.Set("X-Paperboat-Preview-ID", identity)
	request.Header.Set("X-Forwarded-For", "spoofed")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "ok:/hello" {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
	for _, header := range []string{"X-Robots-Tag", "Referrer-Policy", "X-Content-Type-Options"} {
		if response.Header().Get(header) == "" {
			t.Fatalf("missing %s", header)
		}
	}
}

func TestProxyReadinessOutcomes(t *testing.T) {
	registry := newRegistry(t, &fakeProber{}, 1)
	registry.Register("prv", "env", "web", Target{"127.0.0.1", 1}, true)
	proxy, _ := NewProxy(ProxyConfig{Registry: registry, RetryAfter: 1500 * time.Millisecond})
	for _, test := range []struct {
		identity string
		status   int
	}{{"missing", 404}, {"prv", 503}} {
		request := httptest.NewRequest(http.MethodGet, "http://preview.test/", nil)
		request.Header.Set("X-Paperboat-Preview-ID", test.identity)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("identity=%s status=%d", test.identity, response.Code)
		}
		if test.status == 503 && response.Header().Get("Retry-After") != "2" {
			t.Fatalf("retry-after=%q", response.Header().Get("Retry-After"))
		}
	}
}

func TestProxyFlushesSSEAndPropagatesCancellation(t *testing.T) {
	release := make(chan struct{})
	canceled := make(chan struct{})
	targetConnection, closeTarget := servePipe(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: first\n\n"))
		writer.(http.Flusher).Flush()
		select {
		case <-release:
		case <-request.Context().Done():
			close(canceled)
		}
	}))
	defer closeTarget()
	targetTransport := pipeTransport(targetConnection)
	defer targetTransport.CloseIdleConnections()
	proxy, identity := readyProxy(t, targetTransport)
	proxyConnection, closeProxy := servePipe(t, proxy)
	defer closeProxy()
	proxyTransport := pipeTransport(proxyConnection)
	defer proxyTransport.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://preview.test/", nil)
	request.Header.Set("X-Paperboat-Preview-ID", identity)
	response, err := (&http.Client{Transport: proxyTransport}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("line=%q err=%v", line, err)
	}
	cancel()
	_ = response.Body.Close()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("target did not observe cancellation")
	}
	close(release)
}

func TestProxyCarriesHTTPUpgrade(t *testing.T) {
	targetConnection, closeTarget := servePipe(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		hijacker := writer.(http.Hijacker)
		connection, buffer, err := hijacker.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		_, _ = buffer.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		_ = buffer.Flush()
		line, _ := buffer.ReadString('\n')
		_, _ = buffer.WriteString(line)
		_ = buffer.Flush()
	}))
	defer closeTarget()
	targetTransport := pipeTransport(targetConnection)
	defer targetTransport.CloseIdleConnections()
	proxy, identity := readyProxy(t, targetTransport)
	connection, closeProxy := servePipe(t, proxy)
	defer closeProxy()
	_, _ = fmt.Fprintf(connection, "GET / HTTP/1.1\r\nHost: preview.test\r\nConnection: Upgrade\r\nUpgrade: test\r\nX-Paperboat-Preview-ID: %s\r\n\r\n", identity)
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	_, _ = io.WriteString(connection, "ping\n")
	line, err := reader.ReadString('\n')
	if err != nil || line != "ping\n" {
		t.Fatalf("line=%q err=%v", line, err)
	}
}
