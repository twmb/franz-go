package sr

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
)

func TestDialTLSConfig(t *testing.T) {
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
}

func TestDialTLSConfigWithHTTPClient(t *testing.T) {
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
}

func TestDialTLSConfigWithHTTPClientAndTransport(t *testing.T) {
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
}

func TestDialTLSConfigWithNilHTTPClient(t *testing.T) {
	tlscfg := &tls.Config{}

	rcl, err := NewClient(
		HTTPClient(nil),
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
}

func TestDialTLSConfigWithIncompatibleHTTPClient(t *testing.T) {
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

}
