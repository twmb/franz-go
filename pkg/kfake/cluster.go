package kfake

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
//
// * Handle requests concurrently, i.e. JoinGroup
//   * Actually, just spin out concurrent group manager that then hooks back
//     into the control loop
//
// * Add raft and make the brokers independent
//
// * Support multiple replicas -- we just pass this through
// * Support per-partition leader epoch

type (

	// Cluster is a mock Kafka broker cluster.
	Cluster struct {
		cfg cfg

		controller *broker
		bs         []*broker

		adminCh      chan func()
		reqCh        chan clientReq
		watchFetchCh chan *watchFetch

		controlMu          sync.Mutex
		control            map[int16][]controlFn
		keepCurrentControl atomic.Bool

		epoch int32
		data  data
		pids  pids

		die  chan struct{}
		dead atomic.Bool
	}

	broker struct {
		c    *Cluster
		ln   net.Listener
		node int32
	}

	controlFn func(kmsg.Request) (kmsg.Response, error, bool)
)

// NewCluster returns a new mocked Kafka cluster.
func NewCluster(opts ...Opt) (c *Cluster, err error) {
	cfg := cfg{
		nbrokers:        3,
		logger:          new(nopLogger),
		clusterID:       "kfake",
		defaultNumParts: 10,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if len(cfg.ports) > 0 {
		cfg.nbrokers = len(cfg.ports)
	}

	c = &Cluster{
		cfg: cfg,

		adminCh:      make(chan func()),
		reqCh:        make(chan clientReq, 20),
		watchFetchCh: make(chan *watchFetch, 20),

		data: data{
			id2t: make(map[uuid]string),
			t2id: make(map[string]uuid),
		},

		die: make(chan struct{}),
	}
	c.data.c = c
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	for i := 0; i < cfg.nbrokers; i++ {
		var port int
		if len(cfg.ports) > 0 {
			port = cfg.ports[i]
		}
		b := &broker{
			c:    c,
			ln:   newListener(port),
			node: int32(i + 1),
		}
		c.bs = append(c.bs, b)
		go b.listen()
	}
	c.controller = c.bs[len(c.bs)-1]
	go c.run()
	return c, nil
}

// ListenAddrs returns the hostports that the cluster is listening on.
func (c *Cluster) ListenAddrs() []string {
	var addrs []string
	for _, b := range c.bs {
		addrs = append(addrs, b.ln.Addr().String())
	}
	return addrs
}

// Close shuts down the cluster.
func (c *Cluster) Close() {
	if c.dead.Swap(true) {
		return
	}
	close(c.die)
	for _, b := range c.bs {
		b.ln.Close()
	}
}

func newListener(port int) net.Listener {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		if l, err = net.Listen("tcp6", fmt.Sprintf("[::1]:%d")); err != nil {
			panic(fmt.Sprintf("kfake: failed to listen on a port: %v", err))
		}
	}
	return l
}

func (b *broker) listen() {
	defer b.ln.Close()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}

		cc := &clientConn{
			c:      b.c,
			b:      b,
			conn:   conn,
			respCh: make(chan clientResp, 2),
		}
		go cc.read()
		go cc.write()
	}
}

type clientReq struct {
	cc   *clientConn
	kreq kmsg.Request
	at   time.Time
	corr int32
}
type clientResp struct {
	kresp kmsg.Response
	corr  int32
	err   error
}

func (c *Cluster) run() {
	for {
		var creq clientReq
		var w *watchFetch

		select {
		case creq = <-c.reqCh:
		case w = <-c.watchFetchCh:
			if w.cleaned {
				continue // already cleaned up, this is an extraneous timer fire
			}
			w.cleanup(c)
			creq = w.creq
		case <-c.die:
			return
		case fn := <-c.adminCh:
			// Run a custom request in the context of the cluster
			fn()
			continue
		}

		kreq := creq.kreq
		kresp, err, handled := c.tryControl(kreq)
		if handled {
			goto afterControl
		}

		switch k := kmsg.Key(kreq.Key()); k {
		case kmsg.Produce:
			kresp, err = c.handleProduce(creq.cc.b, kreq)
		case kmsg.Fetch:
			kresp, err = c.handleFetch(creq, w)
		case kmsg.ListOffsets:
			kresp, err = c.handleListOffsets(creq.cc.b, kreq)
		case kmsg.Metadata:
			kresp, err = c.handleMetadata(kreq)
		case kmsg.ApiVersions:
			kresp, err = c.handleApiVersions(kreq)
		case kmsg.CreateTopics:
			kresp, err = c.handleCreateTopics(creq.cc.b, kreq)
		case kmsg.DeleteTopics:
			kresp, err = c.handleDeleteTopics(creq.cc.b, kreq)
		case kmsg.InitProducerID:
			kresp, err = c.handleInitProducerID(kreq)
		case kmsg.CreatePartitions:
			kresp, err = c.handleCreatePartitions(creq.cc.b, kreq)
		default:
			err = fmt.Errorf("unahndled key %v", k)
		}

	afterControl:
		if kresp == nil && err == nil { // produce request with no acks
			continue
		}

		select {
		case creq.cc.respCh <- clientResp{kresp: kresp, corr: creq.corr, err: err}:
		case <-c.die:
			return
		}
	}
}

// Control is a function to call on any client request the cluster handles.
//
// If the control function returns true, then either the response is written
// back to the client or, if there the control function returns an error, the
// client connection is closed. If both returns are nil, then the cluster will
// loop continuing to read from the client and the client will likely have a
// read timeout at some point.
//
// Controlling a request drops the control function from the cluster, meaning
// that a control function can only control *one* request. To keep the control
// function handling more requests, you can call KeepControl within your
// control function.
//
// It is safe to add new control functions within a control function. Control
// functions are not called concurrently.
func (c *Cluster) Control(fn func(kmsg.Request) (kmsg.Response, error, bool)) {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	c.control[-1] = append(c.control[-1], fn)
}

// Control is a function to call on a specific request key that the cluster
// handles.
//
// If the control function returns true, then either the response is written
// back to the client or, if there the control function returns an error, the
// client connection is closed. If both returns are nil, then the cluster will
// loop continuing to read from the client and the client will likely have a
// read timeout at some point.
//
// Controlling a request drops the control function from the cluster, meaning
// that a control function can only control *one* request. To keep the control
// function handling more requests, you can call KeepControl within your
// control function.
//
// It is safe to add new control functions within a control function.
func (c *Cluster) ControlKey(key int16, fn func(kmsg.Request) (kmsg.Response, error, bool)) {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	c.control[key] = append(c.control[key], fn)
}

// KeepControl marks the currently running control function to be kept even if
// you handle the request and return true. This can be used to continuously
// control requests without needing to re-add control functions manually.
func (c *Cluster) KeepControl() {
	c.keepCurrentControl.Swap(true)
}

func (c *Cluster) tryControl(kreq kmsg.Request) (kresp kmsg.Response, err error, handled bool) {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	if len(c.control) == 0 {
		return nil, nil, false
	}

	keyFns := c.control[kreq.Key()]
	for i, fn := range keyFns {
		kresp, err, handled = c.callControl(kreq.Key(), kreq, fn)
		if handled {
			c.control[kreq.Key()] = append(keyFns[:i], keyFns[i+1:]...)
			return
		}
	}
	anyFns := c.control[-1]
	for i, fn := range anyFns {
		kresp, err, handled = c.callControl(-1, kreq, fn)
		if handled {
			c.control[-1] = append(anyFns[:i], anyFns[i+1:]...)
			return
		}
	}
	return
}

func (c *Cluster) callControl(key int16, req kmsg.Request, fn controlFn) (kresp kmsg.Response, err error, handled bool) {
	c.keepCurrentControl.Swap(false)
	c.controlMu.Unlock()
	defer func() {
		c.controlMu.Lock()
		if handled && c.keepCurrentControl.Swap(false) {
			c.control[key] = append(c.control[key], fn)
		}
	}()
	return fn(req)
}

// Various administrative requests can be passed into the cluster to simulate
// real-world operations. These are performed synchronously in the goroutine
// that handles client requests.

func (c *Cluster) admin(fn func()) {
	c.adminCh <- fn
}

// MoveTopicPartition simulates the rebalancing of a partition to an alternative
// broker
func (c *Cluster) MoveTopicPartition(topic string, partition int32, bid int32) error {
	var br *broker
	for _, b := range c.bs {
		if b.node == bid {
			br = b
			break
		}
	}
	if br == nil {
		return errors.New("no such broker")
	}

	resp := make(chan error, 1)
	c.admin(func() {
		pd, ok := c.data.tps.getp(topic, partition)
		if !ok {
			resp <- errors.New("topic/partition not found")
			return
		}
		pd.leader = br
		resp <- nil
	})

	return <-resp
}
