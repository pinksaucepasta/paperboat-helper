package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	frpserver "github.com/fatedier/frp/server"
	"github.com/pinksaucepasta/paperboat-helper/internal/connector"
	"github.com/pinksaucepasta/paperboat-helper/internal/filetransfer"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
	"github.com/quic-go/quic-go/http3"
)

func TestRealCaddyFRPFileTransferHTTP3AndHTTP2(t *testing.T) {
	caddyBinary := os.Getenv("PAPERBOAT_REAL_CADDY_BINARY")
	if caddyBinary == "" {
		t.Skip("set PAPERBOAT_REAL_CADDY_BINARY to the pinned custom Caddy binary")
	}
	for _, executable := range []string{caddyBinary} {
		if info, err := os.Stat(executable); err != nil || info.Mode()&0o111 == 0 {
			t.Fatalf("integration executable %q is unavailable", executable)
		}
	}

	root := t.TempDir()
	durable, err := store.Open(context.Background(), store.Config{Root: filepath.Join(root, "state")})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	service, err := filetransfer.New(filetransfer.Config{Root: filepath.Join(root, "files"), Store: durable})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := operation.NewJournal(64)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewFileTransferHandler(FileTransferHandlerConfig{
		Service: service,
		Journal: journal,
		Authorizer: func(token string) (Authorizer, error) {
			if token != "integration-token" {
				return nil, fmt.Errorf("invalid token")
			}
			return authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
				return Authorization{JournalBinding: "integration-binding", EnvironmentID: "env_integration", ClientID: "cli_integration", SessionID: "ses_integration"}, nil
			}), nil
		},
		AllowDirection: func(_ Authorization, direction string) bool {
			return direction == "pb_to_pbh" || direction == "pbh_to_pb"
		},
		ResolveClient: func(_ Authorization, _, _ string) (string, error) { return "cli_integration", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	helperListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	helperHTTP := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthz" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		handler.ServeHTTP(writer, request)
	})}
	go func() { _ = helperHTTP.Serve(helperListener) }()
	defer helperHTTP.Shutdown(context.Background())
	helperPort := helperListener.Addr().(*net.TCPAddr).Port

	controlPort := reserveTCPPort(t)
	vhostPort := reserveTCPPort(t)
	token := strings.Repeat("f", 40)
	serverConfig := &v1.ServerConfig{BindAddr: "127.0.0.1", BindPort: controlPort, QUICBindPort: controlPort, ProxyBindAddr: "127.0.0.1", VhostHTTPPort: vhostPort, Auth: v1.AuthServerConfig{Method: v1.AuthMethodToken, Token: token}}
	if err := serverConfig.Complete(); err != nil {
		t.Fatal(err)
	}
	frps, err := frpserver.NewService(serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	frpsCtx, stopFRPS := context.WithCancel(context.Background())
	go frps.Run(frpsCtx)
	defer func() { stopFRPS(); _ = frps.Close() }()

	dialer, err := connector.NewFRPDialer(connector.FRPDialerConfig{ReadyTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := dialer.Dial(context.Background(), connector.QUIC, connector.Admission{
		OperationID: "op_real_file_transfer", EnvironmentID: "env_integration", HelperID: "helper_integration", Credential: token,
		Endpoint: connector.EdgeEndpoint{Host: "127.0.0.1", Port: uint16(controlPort), QUICPort: uint16(controlPort)},
		Routes:   []connector.RouteHandoff{{RouteID: "route_file_transfer", Revision: 1, Kind: "helper_https_wss", PublicHost: "transfer.test", ProxyName: "file-transfer-integration", LocalTarget: connector.RouteTarget{Host: "127.0.0.1", Port: uint16(helperPort)}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	publicPort := reserveTCPPort(t)
	caddyRoot := filepath.Join(root, "caddy")
	configPath := filepath.Join(root, "caddy.json")
	brokerSocket := filepath.Join("/tmp", "paperboat-integration-"+strconv.Itoa(os.Getpid())+".sock")
	defer os.Remove(brokerSocket)
	config := realCaddyConfig(publicPort, vhostPort, brokerSocket)
	encoded, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(caddyBinary, "run", "--config", configPath)
	command.Env = append(os.Environ(), "XDG_DATA_HOME="+filepath.Join(caddyRoot, "data"), "XDG_CONFIG_HOME="+filepath.Join(caddyRoot, "config"))
	var caddyOutput bytes.Buffer
	command.Stdout, command.Stderr = &caddyOutput, &caddyOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = command.Process.Signal(os.Interrupt)
		wait := make(chan struct{})
		go func() { _ = command.Wait(); close(wait) }()
		select {
		case <-wait:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-wait
		}
	}()

	rootCertificate := filepath.Join(caddyRoot, "data", "caddy", "pki", "authorities", "local", "root.crt")
	waitForFile(t, rootCertificate, 15*time.Second, &caddyOutput)
	certificate, err := os.ReadFile(rootCertificate)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		t.Fatal("Caddy root certificate is invalid")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "transfer.test"}
	h3Transport := &http3.Transport{TLSClientConfig: tlsConfig.Clone()}
	defer h3Transport.Close()
	h2Transport := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: tlsConfig.Clone()}
	defer h2Transport.CloseIdleConnections()
	h3Client := &http.Client{Transport: h3Transport, Timeout: 15 * time.Second}
	h2Client := &http.Client{Transport: h2Transport, Timeout: 15 * time.Second}
	endpoint := "https://127.0.0.1:" + strconv.Itoa(publicPort) + "/v1/file-transfers"
	waitForHTTPS(t, h2Client, "https://127.0.0.1:"+strconv.Itoa(publicPort)+"/healthz", &caddyOutput)

	data := append([]byte("mixed transport\x00"), bytes.Repeat([]byte{0x80, 0xff, 0x01}, 16<<10)...)
	id := createRealTransfer(t, h3Client, endpoint, "fb_real_h3", "pb_to_pbh", data, 3)
	cut := len(data) / 3
	patchRealTransfer(t, h3Client, endpoint, id, 0, data[:cut], 3)
	patchRealTransfer(t, h2Client, endpoint, id, int64(cut), data[cut:], 2)
	completeRealTransfer(t, h2Client, endpoint, id, 2)
	request := realTransferRequest(t, http.MethodGet, endpoint+"/"+id+"/content", nil)
	request.Header.Set("Range", "bytes=7-")
	request.Header.Set("If-Match", `"sha256:`+transferDigest(data)+`"`)
	response, err := h3Client.Do(request)
	if err != nil {
		t.Fatalf("HTTP/3 range download: %v\nCaddy output:\n%s", err, caddyOutput.String())
	}
	got, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusPartialContent || response.ProtoMajor != 3 || !bytes.Equal(got, data[7:]) {
		t.Fatalf("range status=%d proto=%s bytes=%d err=%v", response.StatusCode, response.Proto, len(got), readErr)
	}

	if os.Getenv("PAPERBOAT_REAL_BLOCK_UDP") == "1" {
		blockUDPPort(t, publicPort)
		_ = h3Transport.Close()
		blockedTransport := &http3.Transport{TLSClientConfig: tlsConfig.Clone()}
		blockedClient := &http.Client{Transport: blockedTransport, Timeout: 750 * time.Millisecond}
		blockedRequest, _ := http.NewRequest(http.MethodGet, "https://127.0.0.1:"+strconv.Itoa(publicPort)+"/healthz", nil)
		blockedRequest.Host = "transfer.test"
		if blockedResponse, blockedErr := blockedClient.Do(blockedRequest); blockedErr == nil {
			blockedResponse.Body.Close()
			_ = blockedTransport.Close()
			t.Fatal("HTTP/3 remained reachable after UDP was rejected")
		}
		_ = blockedTransport.Close()
	}

	reverseData := append([]byte("reverse direction\x00"), bytes.Repeat([]byte{0x7f, 0x00, 0xfe}, 8<<10)...)
	reverseID := createRealTransfer(t, h2Client, endpoint, "fb_real_h2", "pbh_to_pb", reverseData, 2)
	patchRealTransfer(t, h2Client, endpoint, reverseID, 0, reverseData, 2)
	completeRealTransfer(t, h2Client, endpoint, reverseID, 2)
	pendingRequest := realTransferRequest(t, http.MethodGet, endpoint+"/pending?session_id=ses_integration&wait_seconds=0", nil)
	pendingResponse, err := h2Client.Do(pendingRequest)
	if err != nil {
		t.Fatal(err)
	}
	var pending struct {
		Transfers []store.FileTransfer `json:"transfers"`
	}
	decodeErr := json.NewDecoder(pendingResponse.Body).Decode(&pending)
	pendingResponse.Body.Close()
	if decodeErr != nil || pendingResponse.StatusCode != http.StatusOK || pendingResponse.ProtoMajor != 2 || len(pending.Transfers) != 1 || pending.Transfers[0].ID != reverseID {
		t.Fatalf("pending status=%d proto=%s transfers=%v err=%v", pendingResponse.StatusCode, pendingResponse.Proto, pending.Transfers, decodeErr)
	}
	downloadRequest := realTransferRequest(t, http.MethodGet, endpoint+"/"+reverseID+"/content", nil)
	downloadRequest.Header.Set("If-Match", `"sha256:`+transferDigest(reverseData)+`"`)
	downloadResponse, err := h2Client.Do(downloadRequest)
	if err != nil {
		t.Fatal(err)
	}
	downloaded, downloadErr := io.ReadAll(downloadResponse.Body)
	downloadResponse.Body.Close()
	if downloadErr != nil || downloadResponse.StatusCode != http.StatusOK || downloadResponse.ProtoMajor != 2 || !bytes.Equal(downloaded, reverseData) {
		t.Fatalf("download status=%d proto=%s bytes=%d err=%v", downloadResponse.StatusCode, downloadResponse.Proto, len(downloaded), downloadErr)
	}
	receiptBody := strings.NewReader(`{"result_code":"stored","path":"Paperboat Inbox/reverse.bin"}`)
	receiptRequest := realTransferRequest(t, http.MethodPost, endpoint+"/"+reverseID+"/receipt", receiptBody)
	receiptRequest.Header.Set("Content-Type", "application/json")
	receiptResponse, err := h2Client.Do(receiptRequest)
	if err != nil {
		t.Fatal(err)
	}
	receiptResponse.Body.Close()
	if receiptResponse.StatusCode != http.StatusNoContent || receiptResponse.ProtoMajor != 2 {
		t.Fatalf("receipt status=%d proto=%s", receiptResponse.StatusCode, receiptResponse.Proto)
	}
}

func blockUDPPort(t *testing.T, port int) {
	t.Helper()
	args := []string{"-I", "OUTPUT", "1", "-p", "udp", "--dport", strconv.Itoa(port), "-j", "REJECT"}
	if output, err := exec.Command("iptables", args...).CombinedOutput(); err != nil {
		t.Fatalf("block UDP: %v: %s", err, output)
	}
	t.Cleanup(func() {
		deleteArgs := []string{"-D", "OUTPUT", "-p", "udp", "--dport", strconv.Itoa(port), "-j", "REJECT"}
		if output, err := exec.Command("iptables", deleteArgs...).CombinedOutput(); err != nil {
			t.Errorf("restore UDP firewall: %v: %s", err, output)
		}
	})
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func realCaddyConfig(publicPort, upstreamPort int, brokerSocket string) map[string]any {
	listen := "127.0.0.1:" + strconv.Itoa(publicPort)
	return map[string]any{
		"admin": map[string]any{"disabled": true},
		"apps": map[string]any{
			"paperboat_quic": map[string]any{"listen": listen, "http_server": "integration", "broker_socket": brokerSocket, "max_connections": 32, "max_connections_per_ip": 8, "max_streams_per_connection": 3},
			"http": map[string]any{"servers": map[string]any{"integration": map[string]any{
				"listen": []string{listen}, "protocols": []string{"h1", "h2", "h3"}, "automatic_https": map[string]any{"disable_redirects": true},
				"routes": []any{map[string]any{"match": []any{map[string]any{"host": []string{"transfer.test"}}}, "handle": []any{map[string]any{"handler": "reverse_proxy", "flush_interval": -1, "upstreams": []any{map[string]any{"dial": "127.0.0.1:" + strconv.Itoa(upstreamPort)}}}}}},
			}}},
			"tls": map[string]any{"automation": map[string]any{"policies": []any{map[string]any{"subjects": []string{"transfer.test"}, "issuers": []any{map[string]any{"module": "internal"}}}}}},
			"pki": map[string]any{"certificate_authorities": map[string]any{"local": map[string]any{"install_trust": false}}},
		},
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s\nCaddy output:\n%s", path, output.String())
}

func waitForHTTPS(t *testing.T, client *http.Client, target string, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		request, _ := http.NewRequest(http.MethodGet, target, nil)
		request.Host = "transfer.test"
		if response, err := client.Do(request); err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Caddy endpoint did not become ready\n%s", output.String())
}

func realTransferRequest(t *testing.T, method, target string, body io.Reader) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "transfer.test"
	request.Header.Set("Authorization", "Bearer integration-token")
	request.Header.Set(HeaderRequestID, "req_real_transfer")
	request.Header.Set(HeaderOperationID, fmt.Sprintf("operation_real_%d", time.Now().UnixNano()))
	return request
}

func createRealTransfer(t *testing.T, client *http.Client, endpoint, batchID, direction string, data []byte, protocolMajor int) string {
	t.Helper()
	payload, _ := json.Marshal(createFileTransferRequest{BatchID: batchID, Direction: direction, SessionID: "ses_integration", Files: []filetransfer.File{{Basename: "opaque.dat", Size: int64(len(data)), SHA256: transferDigest(data)}}})
	request := realTransferRequest(t, http.MethodPost, endpoint, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var created createFileTransferResponse
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil || response.StatusCode != http.StatusCreated || response.ProtoMajor != protocolMajor || len(created.Transfers) != 1 {
		t.Fatalf("create status=%d proto=%s transfers=%d err=%v", response.StatusCode, response.Proto, len(created.Transfers), err)
	}
	return created.Transfers[0].ID
}

func patchRealTransfer(t *testing.T, client *http.Client, endpoint, id string, offset int64, data []byte, protocolMajor int) {
	t.Helper()
	request := realTransferRequest(t, http.MethodPatch, endpoint+"/"+id+"/content", bytes.NewReader(data))
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	request.Header.Set(HeaderUploadOffset, strconv.FormatInt(offset, 10))
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent || response.ProtoMajor != protocolMajor {
		t.Fatalf("patch status=%d proto=%s", response.StatusCode, response.Proto)
	}
}

func completeRealTransfer(t *testing.T, client *http.Client, endpoint, id string, protocolMajor int) {
	t.Helper()
	request := realTransferRequest(t, http.MethodPost, endpoint+"/"+id+"/complete", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ProtoMajor != protocolMajor {
		t.Fatalf("complete status=%d proto=%s", response.StatusCode, response.Proto)
	}
}
