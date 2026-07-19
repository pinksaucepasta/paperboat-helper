package preview

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"
)

type ProxyConfig struct {
	Registry      *Registry
	Transport     http.RoundTripper
	RetryAfter    time.Duration
	FlushInterval time.Duration
}

type Proxy struct {
	registry      *Registry
	transport     http.RoundTripper
	retryAfter    time.Duration
	flushInterval time.Duration
}

func NewProxy(config ProxyConfig) (*Proxy, error) {
	if config.Registry == nil {
		return nil, ErrInvalidTarget
	}
	if config.Transport == nil {
		config.Transport = http.DefaultTransport
	}
	if config.RetryAfter == 0 {
		config.RetryAfter = time.Second
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = -1
	}
	if config.RetryAfter < 0 {
		return nil, ErrInvalidTarget
	}
	return &Proxy{registry: config.Registry, transport: config.Transport, retryAfter: config.RetryAfter, flushInterval: config.FlushInterval}, nil
}

func (p *Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	identity := request.PathValue("preview_id")
	if identity == "" {
		identity = request.Header.Get("X-Paperboat-Preview-ID")
	}
	record, err := p.registry.Get(identity)
	if err != nil {
		writePreviewStatus(writer, http.StatusNotFound, 0)
		return
	}
	switch record.State {
	case Registering, Degraded, Offline:
		writePreviewStatus(writer, http.StatusServiceUnavailable, p.retryAfter)
		return
	case Expired:
		writePreviewStatus(writer, http.StatusGone, 0)
		return
	case Removed:
		writePreviewStatus(writer, http.StatusNotFound, 0)
		return
	case Ready:
	default:
		writePreviewStatus(writer, http.StatusServiceUnavailable, p.retryAfter)
		return
	}
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort(record.Target.Host, strconv.Itoa(int(record.Target.Port)))}
	proxy := &httputil.ReverseProxy{
		Transport:     p.transport,
		FlushInterval: p.flushInterval,
		Rewrite: func(proxyRequest *httputil.ProxyRequest) {
			proxyRequest.SetURL(target)
			proxyRequest.Out.Host = target.Host
			for _, header := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP", "Proxy-Authorization", "X-Paperboat-Preview-ID"} {
				proxyRequest.Out.Header.Del(header)
			}
		},
		ModifyResponse: func(response *http.Response) error {
			applySafetyHeaders(response.Header)
			return nil
		},
		ErrorHandler: func(writer http.ResponseWriter, _ *http.Request, _ error) {
			writePreviewStatus(writer, http.StatusBadGateway, 0)
		},
	}
	proxy.ServeHTTP(writer, request)
}

func writePreviewStatus(writer http.ResponseWriter, status int, retryAfter time.Duration) {
	applySafetyHeaders(writer.Header())
	if retryAfter > 0 {
		seconds := int64((retryAfter + time.Second - 1) / time.Second)
		writer.Header().Set("Retry-After", fmt.Sprint(seconds))
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(status)
}

func applySafetyHeaders(header http.Header) {
	header.Set("X-Robots-Tag", "noindex, nofollow, noarchive")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}
