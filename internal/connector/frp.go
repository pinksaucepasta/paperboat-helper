package connector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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
	Factory               FRPFactory
	ReadyTimeout          time.Duration
	ProxyCheckInterval    time.Duration
	ProxyFailureThreshold int
	RouteKinds            []string
}

type FRPDialer struct {
	config     FRPDialerConfig
	routeKinds map[string]bool
}

func NewFRPDialer(config FRPDialerConfig) (*FRPDialer, error) {
	routeKinds := make(map[string]bool, len(config.RouteKinds))
	for _, kind := range config.RouteKinds {
		if kind != "helper_https_wss" && kind != "preview_public_https_wss" || routeKinds[kind] {
			return nil, ErrFRPInvalid
		}
		routeKinds[kind] = true
	}
	if config.Factory == nil {
		config.Factory = func(admission Admission, transport Transport) (FRPClient, error) {
			return newFRPClientForKinds(admission, transport, routeKinds, nil)
		}
	}
	if config.ReadyTimeout == 0 {
		config.ReadyTimeout = 30 * time.Second
	}
	if config.ProxyCheckInterval == 0 {
		config.ProxyCheckInterval = 250 * time.Millisecond
	}
	if config.ProxyFailureThreshold == 0 {
		config.ProxyFailureThreshold = 3
	}
	if config.ReadyTimeout <= 0 || config.ProxyCheckInterval <= 0 || config.ProxyFailureThreshold < 1 {
		return nil, ErrFRPInvalid
	}
	return &FRPDialer{config: config, routeKinds: routeKinds}, nil
}

func (d *FRPDialer) Dial(ctx context.Context, transport Transport, admission Admission) (Connection, error) {
	client, err := d.config.Factory(admission, transport)
	if err != nil {
		return nil, err
	}
	// The caller's context bounds dial/readiness only. A successful FRP control
	// session must outlive Accept, whose context is canceled immediately after
	// admission completes; Connection.Close owns the established lifetime.
	runCtx, cancel := context.WithCancel(context.Background())
	c := &frpConnection{client: client, cancel: cancel, done: make(chan error, 1), closed: make(chan struct{})}
	go func() { c.done <- client.Run(runCtx) }()
	deadline := time.NewTimer(d.config.ReadyTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	routes := d.selectedRoutes(admission.Routes)
	for {
		ready := true
		for _, route := range routes {
			if !client.ProxyRunning(frpProxyIdentity(admission, route).name) {
				ready = false
				break
			}
		}
		if ready {
			go c.monitorProxies(admission, routes, d.config.ProxyCheckInterval, d.config.ProxyFailureThreshold)
			return c, nil
		}
		select {
		case err := <-c.done:
			_ = c.Close()
			if err == nil {
				err = ErrFRPReady
			}
			return nil, fmt.Errorf("%w: %v", ErrFRPReady, err)
		case <-ctx.Done():
			_ = c.Close()
			return nil, ctx.Err()
		case <-deadline.C:
			_ = c.Close()
			return nil, ErrFRPReady
		case <-ticker.C:
		}
	}
}

func (d *FRPDialer) selectedRoutes(routes []RouteHandoff) []RouteHandoff {
	if len(d.routeKinds) == 0 {
		return routes
	}
	selected := make([]RouteHandoff, 0, len(routes))
	for _, route := range routes {
		if d.routeKinds[route.Kind] {
			selected = append(selected, route)
		}
	}
	return selected
}

type frpConnection struct {
	client FRPClient
	cancel context.CancelFunc
	done   chan error
	closed chan struct{}
	once   sync.Once
}

func (c *frpConnection) monitorProxies(admission Admission, routes []RouteHandoff, interval time.Duration, threshold int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			running := true
			for _, route := range routes {
				if !c.client.ProxyRunning(frpProxyIdentity(admission, route).name) {
					running = false
					break
				}
			}
			if running {
				failures = 0
				continue
			}
			failures++
			if failures >= threshold {
				_ = c.Close()
				return
			}
		}
	}
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
	c.once.Do(func() { close(c.closed); c.cancel(); c.client.Close() })
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
	return newFRPClientForKinds(admission, transport, nil, connectorCreator)
}

func newFRPClientForKinds(admission Admission, transport Transport, routeKinds map[string]bool, connectorCreator func(context.Context, *v1.ClientCommonConfig) frpclient.Connector) (FRPClient, error) {
	configSource := source.NewConfigSource()
	proxies := make([]v1.ProxyConfigurer, 0, len(admission.Routes))
	for _, route := range admission.Routes {
		if len(routeKinds) > 0 && !routeKinds[route.Kind] {
			continue
		}
		identity := frpProxyIdentity(admission, route)
		proxy := &v1.HTTPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: identity.name, Type: "http", ProxyBackend: v1.ProxyBackend{LocalIP: route.LocalTarget.Host, LocalPort: int(route.LocalTarget.Port)}, LoadBalancer: v1.LoadBalancerConfig{Group: identity.group, GroupKey: identity.groupKey}}, DomainConfig: v1.DomainConfig{CustomDomains: []string{route.PublicHost}}}
		proxies = append(proxies, proxy)
	}
	if err := configSource.ReplaceAll(proxies, nil); err != nil {
		return nil, err
	}
	port := connectorPort(admission.Endpoint, transport)
	common := &v1.ClientCommonConfig{ServerAddr: admission.Endpoint.Host, ServerPort: port, LoginFailExit: boolPtr(true), Auth: v1.AuthClientConfig{Method: v1.AuthMethodToken, Token: admission.Credential}}
	// frp hashes Auth.Token before Login reaches the server plugin. The private
	// loopback Paperboat hook needs the signed credential itself for admission;
	// it removes this metadata before frps stores the authenticated session.
	handoff, err := admissionMetadata(admission)
	if err != nil {
		return nil, err
	}
	common.Metadatas = map[string]string{"paperboat.admission": handoff}
	if transport == QUIC {
		common.Transport.Protocol = "quic"
	} else {
		common.Transport.Protocol = "tcp"
		tcpMux := transport == TCPMux
		common.Transport.TCPMux = &tcpMux
	}
	service, err := frpclient.NewService(frpclient.ServiceOptions{Common: common, ConfigSourceAggregator: source.NewAggregator(configSource), ConnectorCreator: connectorCreator})
	if err != nil {
		return nil, err
	}
	return &nativeFRPClient{service: service}, nil
}

func connectorPort(endpoint EdgeEndpoint, transport Transport) int {
	port := endpoint.TCPPort
	if transport == QUIC {
		port = endpoint.QUICPort
	}
	if port == 0 {
		port = endpoint.Port
	}
	return int(port)
}

func admissionMetadata(admission Admission) (string, error) {
	handoff, err := json.Marshal(struct {
		OperationID   string         `json:"operation_id"`
		Credential    string         `json:"credential"`
		EnvironmentID string         `json:"environment_id"`
		HelperID      string         `json:"helper_id"`
		Generation    uint64         `json:"connector_generation"`
		EdgePool      string         `json:"edge_pool"`
		EdgeNodeID    string         `json:"edge_node_id"`
		Routes        []RouteHandoff `json:"routes"`
	}{admission.OperationID, admission.Credential, admission.EnvironmentID, admission.HelperID, admission.Generation, admission.EdgePool, admission.EdgeNodeID, admission.Routes})
	if err != nil || len(handoff) > 64<<10 {
		return "", ErrFRPInvalid
	}
	return string(handoff), nil
}

func boolPtr(value bool) *bool { return &value }

type frpIdentity struct{ name, group, groupKey string }

func frpProxyIdentity(admission Admission, route RouteHandoff) frpIdentity {
	stable := admission.EnvironmentID + "\x00" + admission.HelperID + "\x00" + route.RouteID + "\x00" + route.ProxyName
	physical := stable + "\x00" + admission.OperationID
	return frpIdentity{
		name:     "pbp_" + hashPrefix("paperboat-frp-proxy-v1\x00"+physical, 32),
		group:    "pbg_" + hashPrefix("paperboat-frp-group-v1\x00"+stable, 32),
		groupKey: hashPrefix("paperboat-frp-group-key-v1\x00"+stable, 64),
	}
}

func hashPrefix(value string, length int) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest)[:length]
}
