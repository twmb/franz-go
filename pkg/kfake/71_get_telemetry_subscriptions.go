package kfake

import (
	"encoding/base64"
	"hash/crc32"
	"maps"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// GetTelemetrySubscriptions: v0
//
// KIP-714: Client metrics and observability.
//
// Behavior:
// * No ACL checks (telemetry uses subscription-based validation)
// * Assigns ClientInstanceID if not provided
// * Answers the CLIENT_METRICS subscriptions that match the client, as
//   Kafka does; with none, every metric every 30s (Kafka: none, every 5m)
// * Tracks client instances for PushTelemetry validation
//
// Version notes:
// * v0: Initial version

func init() { regKey(71, 0, 0) }

func (c *Cluster) handleGetTelemetrySubscriptions(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.GetTelemetrySubscriptionsRequest)
	resp := req.ResponseKind().(*kmsg.GetTelemetrySubscriptionsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	id := req.ClientInstanceID
	switch id {
	case [16]byte{}:
		id = randUUID()
	case telemReservedID:
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	tc := c.telemClientFor(creq, id)
	metrics, interval, subID := c.telemSubscription(tc)

	resp.ClientInstanceID = id
	resp.SubscriptionID = subID
	resp.AcceptedCompressionTypes = []int8{4, 3, 1, 2} // zstd, lz4, gzip, snappy: Kafka's preference order
	resp.PushIntervalMillis = interval
	resp.TelemetryMaxBytes = telemMaxBytes
	resp.DeltaTemporality = true
	resp.RequestedMetrics = metrics

	return resp, nil
}

const telemMaxBytes = 1 << 20

// telemReservedID is the instance ID Kafka reserves besides the zero ID.
var telemReservedID = [16]byte{15: 1}

// telemClient is a client as a subscription's match sees it.
type telemClient struct {
	instanceID  [16]byte
	props       map[string]string // match key => value
	version     int               // c.telemVersion when we made this client
	terminating bool              // a push said the client is closing; Push refuses any more
	t           *time.Timer       // forgets the client, see telemClientFor
}

// telemClientFor returns the client for id, making one if we have none or if
// a subscription changed since we made it, as Kafka does. Like Kafka, we
// forget a client max(60s, three push intervals) after its last request.
func (c *Cluster) telemClientFor(creq *clientReq, id [16]byte) telemClient {
	tc, ok := c.telem[id]
	if !ok || tc.version != c.telemVersion {
		if ok {
			tc.t.Stop()
		}
		tc = newTelemClient(creq, id)
		tc.version = c.telemVersion
	}
	_, interval, _ := c.telemSubscription(tc)
	if tc.t != nil {
		tc.t.Stop()
	}
	var t *time.Timer
	t = c.afterFuncOnLoop(max(time.Minute, 3*time.Duration(interval)*time.Millisecond), func() {
		if c.telem[id].t == t { // a timer that fired after a later request reset it is stale
			delete(c.telem, id)
		}
	})
	tc.t = t
	c.telem[id] = tc
	return tc
}

func newTelemClient(creq *clientReq, id [16]byte) telemClient {
	props := map[string]string{
		"client_instance_id":      base64.RawURLEncoding.EncodeToString(id[:]),
		"client_id":               creq.cid,
		"client_software_name":    creq.cc.swName,
		"client_software_version": creq.cc.swVersion,
		"client_source_address":   creq.clientHost(),
	}
	if addr, ok := creq.cc.conn.RemoteAddr().(*net.TCPAddr); ok {
		props["client_source_port"] = strconv.Itoa(addr.Port)
	}
	return telemClient{instanceID: id, props: props}
}

// telemSubscription returns the metrics and push interval for a client and
// the subscription ID that names them. Like Kafka, we answer the union of the
// metrics of every subscription whose match selects the client, and the
// smallest interval; a subscription to "*" is a subscription to every metric.
// The ID changes when the answer does, so a push with an older ID fails with
// UNKNOWN_SUBSCRIPTION_ID and the client gets its subscription again.
//
// Unlike Kafka, which asks for no metrics every five minutes when no
// subscription matches, we then ask for every metric every 30s, so that a
// client pushes without any setup.
func (c *Cluster) telemSubscription(tc telemClient) (metrics []string, intervalMs, subID int32) {
	var (
		matched bool
		all     bool
		union   = make(map[string]bool)
	)
	intervalMs = 300000
	for _, sub := range c.clientMetrics {
		if !tc.matches(sub["match"]) {
			continue
		}
		matched = true
		for _, m := range splitConfigList(sub["metrics"]) {
			all = all || m == "*"
			union[m] = true
		}
		if v := sub["interval.ms"]; v != nil {
			if n, err := strconv.Atoi(strings.TrimSpace(*v)); err == nil {
				intervalMs = min(intervalMs, int32(n))
			}
		}
	}
	switch {
	case !matched:
		metrics, intervalMs = []string{"*"}, 30000
	case all:
		metrics = []string{"*"} // as Kafka answers for every metric
	default:
		metrics = slices.Sorted(maps.Keys(union))
	}

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	h.Write([]byte(strings.Join(metrics, ",") + ";" + strconv.Itoa(int(intervalMs)) + ";"))
	h.Write(tc.instanceID[:])
	return metrics, intervalMs, int32(h.Sum32())
}

// matches returns whether every key=regex in a subscription's match config
// fully matches the client; an empty match matches every client. Kafka splits
// each entry at its first "=".
func (tc telemClient) matches(match *string) bool {
	for _, m := range splitConfigList(match) {
		k, re, _ := strings.Cut(m, "=")
		v, ok := tc.props[strings.TrimSpace(k)]
		if !ok {
			return false
		}
		r, err := regexp.Compile("^(?:" + strings.TrimSpace(re) + ")$")
		if err != nil || !r.MatchString(v) {
			return false
		}
	}
	return true
}

// splitConfigList splits a list config value at commas and trims each
// element; a nil or empty value is an empty list.
func splitConfigList(v *string) []string {
	if v == nil || strings.TrimSpace(*v) == "" {
		return nil
	}
	parts := strings.Split(*v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
