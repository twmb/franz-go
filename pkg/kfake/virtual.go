package kfake

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

)

// delayedConn wraps a net.Conn and adds artificial
// delays to Read/Write operations.
type delayedConn struct {
	net.Conn
	delay time.Duration
}

// Read adds a delay before reading from the underlying connection
func (dc *delayedConn) Read(b []byte) (n int, err error) {
	if dc.delay > 0 {
		time.Sleep(dc.delay)
	}
	return dc.Conn.Read(b)
}

// Write adds a delay before writing to the underlying connection
func (dc *delayedConn) Write(b []byte) (n int, err error) {
	if dc.delay > 0 {
		time.Sleep(dc.delay)
	}
	return dc.Conn.Write(b)
}

// VirtualNetworkingStack represents an in-memory networking stack.
// You can listen to `localhost:port` and then you can dial to it.
// When you Dial(), you will be given a pipe and the server will receive
// the other end of the pipe.
type VirtualNetworkingStack struct {
	mu        sync.RWMutex
	rng       *rand.Rand
	listeners map[int]*VirtualListener
	delay     time.Duration
}

// NewFakeNetwork creates a new fake networking stack
func NewVirtualNetworkingStack(delay time.Duration) *VirtualNetworkingStack {
	return &VirtualNetworkingStack{
		listeners: make(map[int]*VirtualListener),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		delay:     delay,
	}
}

// VirtualListener implements net.Listener using channels
type VirtualListener struct {
	port        int
	stack       *VirtualNetworkingStack
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

type VirtualDialer struct {
	stack *VirtualNetworkingStack
	opts  DialerOpts
}

func (d *VirtualDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *VirtualDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	connection, err := d.stack.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("could not establish connection: %w", err)
	}
	return connection, nil
}

// FakeAddr implements net.Addr
type FakeAddr struct {
	port int
}

func (a *FakeAddr) Network() string { return "fake" }
func (a *FakeAddr) String() string  { return fmt.Sprintf("localhost:%d", a.port) }

func (s *VirtualNetworkingStack) SetDelay(delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay = delay
}

func (s *VirtualNetworkingStack) Listen(network, address string) (net.Listener, error) {
	host, portS, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("failed to split host and port: %w", err)
	}
	if host != "localhost" && host != "127.0.0.1" && host != "" {
		return nil, fmt.Errorf("the virtual network works only on localhost")
	}
	port, err := strconv.Atoi(portS)
	if err != nil {
		return nil, fmt.Errorf("failed to convert port to int: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.listeners[port]; ok {
		return nil, fmt.Errorf("port %d already in use", port)
	}

	listener := &VirtualListener{
		port:        port,
		stack:       s,
		connections: make(chan net.Conn),
		closed:      make(chan struct{}),
	}

	s.listeners[port] = listener
	return listener, nil
}

func (s *VirtualNetworkingStack) Dialer(opts DialerOpts) Dialer {
	return &VirtualDialer{stack: s, opts: opts}
}

func (s *VirtualNetworkingStack) Dial(network, address string) (net.Conn, error) {
	return s.DialContext(context.Background(), network, address)
}

func (s *VirtualNetworkingStack) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, portS, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("failed to split host and port: %w", err)
	}
	if host != "localhost" && host != "127.0.0.1" {
		return nil, fmt.Errorf("the virtual network works only on localhost")
	}
	port, err := strconv.Atoi(portS)
	if err != nil {
		return nil, fmt.Errorf("failed to convert port to int: %w", err)
	}
	s.mu.RLock()
	listener, exists := s.listeners[port]
	delay := s.delay
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no listener on port %d", port)
	}

	// Check if listener is closed
	select {
	case <-listener.closed:
		return nil, fmt.Errorf("listener on port %d is closed", port)
	default:
	}

	// Create a bidirectional pipe
	serverConn, clientConn := net.Pipe()

	delayedServerConn := &delayedConn{
		Conn:  serverConn,
		delay: delay,
	}
	delayedClientConn := &delayedConn{
		Conn:  clientConn,
		delay: delay,
	}

	select {
	case listener.connections <- delayedServerConn:
		return delayedClientConn, nil
	case <-listener.closed:
		delayedServerConn.Close()
		delayedClientConn.Close()
		return nil, fmt.Errorf("listener on port %d is closed", port)
	case <-ctx.Done():
		delayedServerConn.Close()
		delayedClientConn.Close()
		return nil, ctx.Err()
	}
}

func (s *VirtualNetworkingStack) deregister(l *VirtualListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.listeners, l.port)
}

func (l *VirtualListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.closed:
		return nil, errors.New("listener closed")
	}
}

func (l *VirtualListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
		l.stack.deregister(l)
	})
	return nil
}

func (l *VirtualListener) Addr() net.Addr {
	return &FakeAddr{port: l.port}
}

func (s *VirtualNetworkingStack) ActivePorts() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ports := make([]int, 0, len(s.listeners))
	for port := range s.listeners {
		ports = append(ports, port)
	}
	return ports
}

func (s *VirtualNetworkingStack) IsVirtual() bool {
	return true
}
