package connector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	frpclient "github.com/fatedier/frp/client"
	"github.com/fatedier/frp/pkg/config/source"
	v1 "github.com/fatedier/frp/pkg/config/v1"
)

var (
	ErrFRPInvalid = errors.New("invalid frp connector configuration")
	ErrFRPReady   = errors.New("frp connector readiness failed")
)

type FRPClient interface {
	Run(context.Context) error
	Close()
	GracefulClose(time.Duration)
	ProxyRunning(string) bool
}

type FRPFactory func(Admission, Transport) (FRPClient, error)

type FRPDialerConfig struct {
	Factory      FRPFactory
	ReadyTimeout time.Duration
}

type FRPDialer struct{ config FRPDialerConfig }

func NewFRPDialer(config FRPDialerConfig) (*FRPDialer, error) {
	if config.Factory == nil {
		config.Factory = newFRPClient
	}
	if config.ReadyTimeout == 0 {
		config.ReadyTimeout = 30 * time.Second
	}
	if config.ReadyTimeout <= 0 {
		return nil, ErrFRPInvalid
	}
	return &FRPDialer{config: config}, nil
}

func (d *FRPDialer) Dial(ctx context.Context, transport Transport, admission Admission) (Connection, error) {
	client, err := d.config.Factory(admission, transport)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	c := &frpConnection{client: client, cancel: cancel, done: make(chan error, 1)}
	go func() { c.done <- client.Run(runCtx) }()
	deadline := time.NewTimer(d.config.ReadyTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		for _, route := range admission.Routes {
			if !client.ProxyRunning(route.ProxyName) {
				ready = false
				break
			}
		}
		if ready {
			return c, nil
		}
		select {
		case err := <-c.done:
			cancel()
			if err == nil {
				err = ErrFRPReady
			}
			return nil, fmt.Errorf("%w: %v", ErrFRPReady, err)
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		case <-deadline.C:
			cancel()
			return nil, ErrFRPReady
		case <-ticker.C:
		}
	}
}

type frpConnection struct {
	client FRPClient
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func (c *frpConnection) Drain(ctx context.Context) error {
	duration := time.Duration(0)
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		// Upstream GracefulClose sleeps synchronously for the requested drain
		// period before it starts control-session teardown. Reserve half of the
		// caller's bound for that teardown instead of consuming the deadline.
		duration = remaining / 2
		if duration < 0 {
			duration = 0
		}
	}
	c.client.GracefulClose(duration)
	select {
	case err := <-c.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *frpConnection) Close() error {
	c.once.Do(func() { c.cancel(); c.client.Close() })
	return nil
}
func (c *frpConnection) Done() <-chan error { return c.done }

type nativeFRPClient struct{ service *frpclient.Service }

func (c *nativeFRPClient) Run(ctx context.Context) error { return c.service.Run(ctx) }
func (c *nativeFRPClient) Close()                        { c.service.Close() }
func (c *nativeFRPClient) GracefulClose(d time.Duration) { c.service.GracefulClose(d) }
func (c *nativeFRPClient) ProxyRunning(name string) bool {
	status, ok := c.service.StatusExporter().GetProxyStatus(name)
	return ok && status != nil && status.Phase == "running"
}

func newFRPClient(admission Admission, transport Transport) (FRPClient, error) {
	return newFRPClientWithConnector(admission, transport, nil)
}

func newFRPClientWithConnector(admission Admission, transport Transport, connectorCreator func(context.Context, *v1.ClientCommonConfig) frpclient.Connector) (FRPClient, error) {
	configSource := source.NewConfigSource()
	proxies := make([]v1.ProxyConfigurer, 0, len(admission.Routes))
	for _, route := range admission.Routes {
		proxy := &v1.HTTPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: route.ProxyName, Type: "http", ProxyBackend: v1.ProxyBackend{LocalIP: route.LocalTarget.Host, LocalPort: int(route.LocalTarget.Port)}}, DomainConfig: v1.DomainConfig{CustomDomains: []string{route.PublicHost}}}
		proxies = append(proxies, proxy)
	}
	if err := configSource.ReplaceAll(proxies, nil); err != nil {
		return nil, err
	}
	port := int(admission.Endpoint.Port)
	common := &v1.ClientCommonConfig{ServerAddr: admission.Endpoint.Host, ServerPort: port, LoginFailExit: boolPtr(true), Auth: v1.AuthClientConfig{Method: v1.AuthMethodToken, Token: admission.Credential}}
	if transport == QUIC {
		common.Transport.Protocol = "quic"
	} else {
		common.Transport.Protocol = "tcp"
	}
	service, err := frpclient.NewService(frpclient.ServiceOptions{Common: common, ConfigSourceAggregator: source.NewAggregator(configSource), ConnectorCreator: connectorCreator})
	if err != nil {
		return nil, err
	}
	return &nativeFRPClient{service: service}, nil
}

func boolPtr(value bool) *bool { return &value }
