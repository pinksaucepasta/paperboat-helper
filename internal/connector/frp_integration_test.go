package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	frpserver "github.com/fatedier/frp/server"
)

func TestPinnedFRPRealServerHTTPWorkConnection(t *testing.T) {
	if os.Getenv("PAPERBOAT_REAL_FRP_TEST") != "1" {
		t.Skip("set PAPERBOAT_REAL_FRP_TEST=1 in an isolated network namespace")
	}
	for index, transport := range []Transport{TCPTLS, QUIC} {
		t.Run(string(transport), func(t *testing.T) {
			controlPort := 17070 + index
			vhostPort := 18080 + index
			token := strings.Repeat(string('a'+rune(index)), 40)
			serverConfig := &v1.ServerConfig{
				BindAddr: "127.0.0.1", BindPort: controlPort, ProxyBindAddr: "127.0.0.1",
				VhostHTTPPort: vhostPort, Auth: v1.AuthServerConfig{Method: v1.AuthMethodToken, Token: token},
			}
			if transport == QUIC {
				serverConfig.QUICBindPort = controlPort
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
					// v0.70.0 frps Run does not reliably return after Close. This
					// opt-in test runs in an isolated process; Phase 2 asserts only
					// the embedded client work path and bounded client drain.
				}
			})

			target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "text/plain")
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
			replacementAdmission := admission
			replacementAdmission.OperationID = "op_admit_second"
			replacement, err := dialer.Dial(context.Background(), transport, replacementAdmission)
			if err != nil {
				_ = connection.Close()
				t.Fatal(err)
			}
			retireCtx, cancelRetire := context.WithTimeout(context.Background(), 500*time.Millisecond)
			if err := connection.Drain(retireCtx); err != nil {
				cancelRetire()
				_ = replacement.Close()
				t.Fatal(err)
			}
			cancelRetire()
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
			drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelDrain()
			if err := replacement.Drain(drainCtx); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}
