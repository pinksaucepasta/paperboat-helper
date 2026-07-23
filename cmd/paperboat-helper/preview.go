package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
)

const maxPreviewResponseBytes = 1 << 20

func runPreview(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("preview requires create, list, or remove")
	}
	var payload map[string]any
	switch args[0] {
	case "create":
		flags := flag.NewFlagSet("preview create", flag.ContinueOnError)
		flags.SetOutput(stderr)
		name := flags.String("name", "", "Stable preview name")
		port := flags.Int("port", 0, "Local target port")
		public := flags.Bool("public", false, "Acknowledge public access")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errors.New("preview create accepts --name, --port, and --public")
		}
		if *name == "" || *port < 1 || *port > 65535 || !*public {
			return errors.New("preview create requires --name, --port, and --public acknowledgement")
		}
		payload = map[string]any{"action": "create", "logical_name": *name, "target_port": *port, "public_acknowledgement": true}
	case "list":
		if len(args) != 1 {
			return errors.New("preview list does not accept arguments")
		}
		payload = map[string]any{"action": "list"}
	case "remove":
		if len(args) != 2 || strings.TrimSpace(args[1]) == "" {
			return errors.New("preview remove requires one preview name")
		}
		payload = map[string]any{"action": "remove", "logical_name": args[1]}
	default:
		return fmt.Errorf("unknown preview command %q", args[0])
	}

	data, err := callAgentPreview(ctx, payload)
	if err != nil {
		return err
	}
	if args[0] == "list" {
		var records []preview.ControlRecord
		if json.Unmarshal(data, &records) != nil {
			return errors.New("helper returned an invalid preview list")
		}
		if len(records) == 0 {
			fmt.Fprintln(stdout, "No active previews.")
			return nil
		}
		for _, record := range records {
			fmt.Fprintf(stdout, "%s\t%s\t%s\n", record.LogicalName, record.State, record.URL)
		}
		return nil
	}
	var record preview.ControlRecord
	if json.Unmarshal(data, &record) != nil || record.LogicalName == "" || record.URL == "" {
		return errors.New("helper returned an invalid preview")
	}
	if args[0] == "create" {
		fmt.Fprintf(stdout, "%s\nPublic preview: anyone with this URL can access it.\n", record.URL)
	} else {
		fmt.Fprintf(stdout, "Removed preview %s.\n", record.LogicalName)
	}
	return nil
}

func callAgentPreview(ctx context.Context, payload map[string]any) (json.RawMessage, error) {
	endpoint, token, err := agentPreviewConfiguration()
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("preview endpoint redirected") }}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("contact local helper: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxPreviewResponseBytes+1))
	if err != nil || len(responseBody) > maxPreviewResponseBytes {
		return nil, errors.New("helper preview response is unavailable")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("helper preview operation failed with status %d", response.StatusCode)
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF || len(envelope.Data) == 0 {
		return nil, errors.New("helper returned an invalid preview response")
	}
	return envelope.Data, nil
}

func agentPreviewConfiguration() (string, string, error) {
	endpoint := os.Getenv("PAPERBOAT_HELPER_AGENT_ENDPOINT")
	tokenFile := os.Getenv("PAPERBOAT_HELPER_AGENT_TOKEN_FILE")
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "/v1/agent/previews" || u.RawQuery != "" || u.Fragment != "" {
		return "", "", errors.New("local helper preview endpoint is unavailable")
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || port == "" {
		return "", "", errors.New("local helper preview endpoint is unavailable")
	}
	if tokenFile == "" || !filepath.IsAbs(tokenFile) || filepath.Clean(tokenFile) != tokenFile {
		return "", "", errors.New("local helper preview authorization is unavailable")
	}
	info, err := os.Lstat(tokenFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() > 4096 {
		return "", "", errors.New("local helper preview authorization is unavailable")
	}
	data, err := os.ReadFile(tokenFile)
	token := strings.TrimSpace(string(data))
	if err != nil || len(token) < 32 || len(token) > 512 {
		return "", "", errors.New("local helper preview authorization is unavailable")
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", "", errors.New("local helper preview endpoint is unavailable")
	}
	return u.String(), token, nil
}
