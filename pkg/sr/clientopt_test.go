package sr

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
)

func TestDialTLSConfigOpt(t *testing.T) {
	t.Run("Defaults", func(t *testing.T) {
		tlscfg := &tls.Config{}

		rcl, err := NewClient(DialTLSConfig(tlscfg))
		if err != nil {
			t.Fatalf("unable to create client: %s", err)
		}

		if rcl.dialTLS != tlscfg {
			t.Errorf("TLS config: expected %+v, got %+v", tlscfg, rcl.dialTLS)
		}

		tr := rcl.httpcl.Transport.(*http.Transport)
		if tr.TLSClientConfig != tlscfg {
			t.Errorf("TLS config in transport: expected %+v, got %+v", tlscfg, tr.TLSClientConfig)
		}
	})

	t.Run("CustomHTTPClient", func(t *testing.T) {
		httpcl := &http.Client{}
		tlscfg := &tls.Config{}

		rcl, err := NewClient(
			HTTPClient(httpcl),
			DialTLSConfig(tlscfg),
		)
		if err != nil {
			t.Fatalf("unable to create client: %s", err)
		}

		if rcl.dialTLS != tlscfg {
			t.Errorf("TLS config: expected %+v, got %+v", tlscfg, rcl.dialTLS)
		}

		tr := rcl.httpcl.Transport.(*http.Transport)
		if tr.TLSClientConfig != tlscfg {
			t.Errorf("TLS config in transport: expected %+v, got %+v", tlscfg, tr.TLSClientConfig)
		}
	})

	t.Run("CustomHTTPTransport", func(t *testing.T) {
		httptr := &http.Transport{}
		httpcl := &http.Client{Transport: httptr}
		tlscfg := &tls.Config{}

		rcl, err := NewClient(
			HTTPClient(httpcl),
			DialTLSConfig(tlscfg),
		)
		if err != nil {
			t.Fatalf("unable to create client: %s", err)
		}

		if rcl.dialTLS != tlscfg {
			t.Errorf("TLS config: expected %+v, got %+v", tlscfg, rcl.dialTLS)
		}

		tr := rcl.httpcl.Transport.(*http.Transport)
		if tr.TLSClientConfig != tlscfg {
			t.Errorf("TLS config in transport: expected %+v, got %+v", tlscfg, tr.TLSClientConfig)
		}
	})

	t.Run("IncompatibleHTTPClient", func(t *testing.T) {
		dialFn := func(network, addr string) (net.Conn, error) { return nil, nil }
		httptr := &http.Transport{Dial: dialFn}
		httpcl := &http.Client{Transport: httptr}
		tlscfg := &tls.Config{}

		_, err := NewClient(
			HTTPClient(httpcl),
			DialTLSConfig(tlscfg),
		)

		expected := "unable to use DialTLSConfig with an http.Client that has a custom dial function"
		if err.Error() != expected {
			t.Errorf("new client error: expected %q, got %q", expected, err.Error())
		}
	})
}
