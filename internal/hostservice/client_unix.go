//go:build darwin || linux

package hostservice

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

type Client struct {
	socketPath string
	timeout    time.Duration
}

func NewClient(socketPath string, timeout time.Duration) (*Client, error) {
	if !filepath.IsAbs(socketPath) || timeout <= 0 || timeout > 2*time.Minute {
		return nil, ErrInvalidConfig
	}
	return &Client{socketPath: socketPath, timeout: timeout}, nil
}

func (c *Client) Activate(ctx context.Context, worker, host bootstrap.ArtifactManifest) (string, error) {
	dialer := net.Dialer{Timeout: c.timeout}
	connection, err := dialer.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return "", err
	}
	defer connection.Close()
	deadline := time.Now().Add(c.timeout)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	_ = connection.SetDeadline(deadline)
	request := Request{Schema: ProtocolV1, Operation: "activate_update", WorkerArtifact: &worker, HostServiceArtifact: &host}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return "", err
	}
	if closer, ok := connection.(interface{ CloseWrite() error }); !ok || closer.CloseWrite() != nil {
		return "", ErrInvalidRequest
	}
	decoder := json.NewDecoder(io.LimitReader(connection, 16<<10))
	decoder.DisallowUnknownFields()
	var response Response
	var extra any
	if decoder.Decode(&response) != nil || decoder.Decode(&extra) != io.EOF || response.Schema != ProtocolV1 || response.Scope != "system" || response.HostServiceVersion == "" {
		return "", ErrInvalidRequest
	}
	if response.ErrorCode != "" {
		return "", errors.New(response.ErrorCode)
	}
	if response.UpdateVersion == "" || response.UpdateVersion != worker.Version || worker.Version != host.Version {
		return "", ErrInvalidRequest
	}
	return response.UpdateVersion, nil
}
