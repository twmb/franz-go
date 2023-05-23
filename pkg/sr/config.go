package sr

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"
)

type (
	// Opt is an option to configure a client.
	Opt interface{ apply(*Client) }
	opt struct{ fn func(*Client) }
)

func (o opt) apply(cl *Client) { o.fn(cl) }

// HTTPClient sets the http client that the schema registry client uses,
// overriding the default client that speaks plaintext with a timeout of 5s.
func HTTPClient(httpcl *http.Client) Opt {
	return opt{func(cl *Client) { cl.httpcl = httpcl }}
}

// UserAgent sets the User-Agent to use in requests, overriding the default
// "franz-go".
func UserAgent(ua string) Opt {
	return opt{func(cl *Client) { cl.ua = ua }}
}

// URLs sets the URLs that the client speaks to, overriding the default
// http://localhost:8081. This option automatically prefixes any URL that is
// missing an http:// or https:// prefix with http://.
func URLs(urls ...string) Opt {
	return opt{func(cl *Client) {
		for i, u := range urls {
			if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
				continue
			}

			urls[i] = "http://" + u
		}
		cl.urls = urls
	}}
}

// DialTLSConfig sets a tls.Config to use in the default http client.
func DialTLSConfig(c *tls.Config) Opt {
	return opt{func(cl *Client) {
		cl.httpcl = &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSClientConfig:       c,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		}
	}}
}

// Normalize sets the client to add the "?normalize=true" query parameter when
// getting or creating schemas. This can help collapse duplicate schemas into
// one, but can also be done with a configuration parameter on the schema
// registry itself.
func Normalize() Opt {
	return opt{func(cl *Client) { cl.normalize = true }}
}

// BasicAuth sets basic authorization to use for every request.
func BasicAuth(user, pass string) Opt {
	return opt{func(cl *Client) {
		cl.basicAuth = &struct {
			user string
			pass string
		}{user, pass}
	}}
}
