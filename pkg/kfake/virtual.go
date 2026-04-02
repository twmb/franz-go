package kfake

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)

// VirtualNetwork represents an in-memory networking stack.
// You can listen to `localhost:port` and then you can dial to it.
// When you Dial(), you will be given a pipe and the server will receive
// the other end of the pipe.
type VirtualNetwork struct {
	mu        sync.RWMutex
	listeners map[int]*virtualListener
}

func NewVirtualNetwork() *VirtualNetwork {
	return &VirtualNetwork{
		listeners: make(map[int]*virtualListener),
	}
}

// virtualListener implements net.Listener using channels
type virtualListener struct {
	port        int
	stack       *VirtualNetwork
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

// fakeAddr implements net.Addr
type fakeAddr struct {
	port int
}

func (a *fakeAddr) Network() string { return "fake" }
func (a *fakeAddr) String() string  { return fmt.Sprintf("localhost:%d", a.port) }

func (s *VirtualNetwork) Listen(network, address string) (net.Listener, error) {
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

	listener := &virtualListener{
		port:        port,
		stack:       s,
		connections: make(chan net.Conn),
		closed:      make(chan struct{}),
	}

	s.listeners[port] = listener
	return listener, nil
}

func (s *VirtualNetwork) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
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
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no listener on port %d", port)
	}

	select {
	case <-listener.closed:
		return nil, fmt.Errorf("listener on port %d is closed", port)
	default:
	}

	serverConn, clientConn := net.Pipe()

	select {
	case listener.connections <- serverConn:
		return clientConn, nil
	case <-listener.closed:
		serverConn.Close()
		clientConn.Close()
		return nil, fmt.Errorf("listener on port %d is closed", port)
	case <-ctx.Done():
		serverConn.Close()
		clientConn.Close()
		return nil, ctx.Err()
	}
}

func (s *VirtualNetwork) deregister(l *virtualListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.listeners, l.port)
}

func (l *virtualListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.closed:
		return nil, errors.New("listener closed")
	}
}

func (l *virtualListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
		l.stack.deregister(l)
	})
	return nil
}

func (l *virtualListener) Addr() net.Addr {
	return &fakeAddr{port: l.port}
}
