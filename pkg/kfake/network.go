package kfake

import (
	"context"
	"net"
	"time"
)

type Dialer interface {
	Dial(network, address string) (net.Conn, error)
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type DialerOpts struct {
	Timeout   time.Duration
	KeepAlive time.Duration
}

func (d DialerOpts) WithDefaults() DialerOpts {
	if d.Timeout == 0 {
		d.Timeout = 5 * time.Second
	}
	// 0 is a valid value for keepalive, so we don't need to set a default.
	// it defaults to 15 seconds when set to 0.
	return d
}

type NetworkingStack interface {
	Dialer(opts DialerOpts) Dialer
	Listen(network, address string) (net.Listener, error)
	IsVirtual() bool
}

type physicalNetwork struct{}

func (physicalNetwork) Dialer(opts DialerOpts) Dialer {
	var actualOpts = opts.WithDefaults()
	return &net.Dialer{
		Timeout:   actualOpts.Timeout,
		KeepAlive: actualOpts.KeepAlive,
	}
}

func (physicalNetwork) Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func (physicalNetwork) IsVirtual() bool {
	return false
}

func NewPhysicalNetworkingStack() NetworkingStack {
	return physicalNetwork{}
}
