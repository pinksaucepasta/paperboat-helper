package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	frpserver "github.com/fatedier/frp/server"
)

func TestPinnedFRPRealServerHTTPWorkConnection(t *testing.T) {
	if os.Getenv("PAPERBOAT_REAL_FRP_TEST") != "1" {
		t.Skip("set PAPERBOAT_REAL_FRP_TEST=1 in an isolated network namespace")
	}
	for index, transport := range []Transport{TCPMux, TCPDedicated, QUIC} {
		t.Run(string(transport), func(t *testing.T) {
			streamStarted := make(chan struct{})
			finishStream := make(chan struct{})
			releaseStream := sync.OnceFunc(func() { close(finishStream) })
			defer releaseStream()
			controlPort := 17070 + index
			vhostPort := 18080 + index
			token := strings.Repeat(string('a'+rune(index)), 40)
			serverConfig := &v1.ServerConfig{
				BindAddr: "127.0.0.1", BindPort: controlPort, ProxyBindAddr: "127.0.0.1",
				VhostHTTPPort: vhostPort, Auth: v1.AuthServerConfig{Method: v1.AuthMethodToken, Token: token},
			}
			if transport == QUIC {
				serverConfig.QUICBindPort = controlPort
			} else {
				tcpMux := transport == TCPMux
				serverConfig.Transport.TCPMux = &tcpMux
			}
			if err := serverConfig.Complete(); err != nil {
				t.Fatal(err)
			}
			frps, err := frpserver.NewService(serverConfig)
			if err != nil {
				t.Fatal(err)
			}
			serverCtx, cancelServer := context.WithCancel(context.Background())
			serverDone := make(chan struct{})
			go func() { frps.Run(serverCtx); close(serverDone) }()
			t.Cleanup(func() {
				cancelServer()
				_ = frps.Close()
				select {
				case <-serverDone:
				case <-time.After(100 * time.Millisecond):
					// frps Run does not reliably return after Close. This
					// opt-in test runs in an isolated process; The test asserts only
					// the embedded client work path and bounded client drain.
				}
			})

			target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "text/plain")
				if request.URL.Path == "/held" {
					_, _ = io.WriteString(writer, "held-start\n")
					writer.(http.Flusher).Flush()
					close(streamStarted)
					<-finishStream
					_, _ = io.WriteString(writer, "held-end\n")
					return
				}
				_, _ = fmt.Fprintf(writer, "work-connection:%s", request.URL.Path)
			}))
			defer target.Close()
			targetURL, _ := url.Parse(target.URL)
			targetPort, _ := strconv.Atoi(targetURL.Port())
			host := fmt.Sprintf("preview-%d.test", index)
			admission := Admission{
				OperationID: "op_admit_first", EnvironmentID: "env_test", HelperID: "helper_test", Credential: token, Endpoint: EdgeEndpoint{Host: "127.0.0.1", Port: uint16(controlPort)},
				Routes: []RouteHandoff{{RouteID: fmt.Sprintf("route_%d", index), Revision: 1, Kind: "preview_public_https_wss", PublicHost: host, ProxyName: fmt.Sprintf("proxy_%d", index), LocalTarget: RouteTarget{Host: "127.0.0.1", Port: uint16(targetPort)}}},
			}
			dialer, err := NewFRPDialer(FRPDialerConfig{ReadyTimeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			connection, err := dialer.Dial(context.Background(), transport, admission)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/ready", vhostPort), nil)
			request.Host = host
			response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
			if err != nil {
				_ = connection.Close()
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 1024))
			response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusOK || string(body) != "work-connection:/ready" {
				_ = connection.Close()
				t.Fatalf("status=%d body=%q err=%v", response.StatusCode, body, readErr)
			}
			heldRequest, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/held", vhostPort), nil)
			heldRequest.Host = host
			heldResponse, err := (&http.Client{Timeout: 30 * time.Second}).Do(heldRequest)
			if err != nil {
				_ = connection.Close()
				t.Fatal(err)
			}
			<-streamStarted
			if err := connection.Retire(); err != nil {
				heldResponse.Body.Close()
				_ = connection.Close()
				t.Fatal(err)
			}
			fencedResponse, fencedErr := (&http.Client{Timeout: time.Second}).Do(request)
			if fencedErr == nil {
				fencedResponse.Body.Close()
				if fencedResponse.StatusCode == http.StatusOK {
					t.Fatal("retired connector accepted new traffic")
				}
			}
			replacementAdmission := admission
			replacementAdmission.OperationID = "op_admit_second"
			replacement, err := dialer.Dial(context.Background(), transport, replacementAdmission)
			if err != nil {
				_ = connection.Close()
				t.Fatal(err)
			}
			response, err = (&http.Client{Timeout: 5 * time.Second}).Do(request)
			if err != nil {
				_ = replacement.Close()
				t.Fatal(err)
			}
			body, readErr = io.ReadAll(io.LimitReader(response.Body, 1024))
			response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusOK || string(body) != "work-connection:/ready" {
				_ = replacement.Close()
				t.Fatalf("replacement status=%d body=%q err=%v", response.StatusCode, body, readErr)
			}
			releaseStream()
			heldBody, heldErr := io.ReadAll(io.LimitReader(heldResponse.Body, 1024))
			heldResponse.Body.Close()
			if heldErr != nil || string(heldBody) != "held-start\nheld-end\n" {
				t.Fatalf("retired stream body=%q err=%v", heldBody, heldErr)
			}
			_ = connection.Close()
			lifecycle, ok := replacement.(LifecycleConnection)
			if !ok {
				t.Fatal("real FRP connection does not expose lifecycle")
			}
			_ = frps.Close()
			select {
			case <-lifecycle.Done():
			case <-time.After(5 * time.Second):
				_ = replacement.Close()
				t.Fatal("FRP client retried consumed admission after control loss")
			}
			_ = replacement.Close()
		})
	}
}
