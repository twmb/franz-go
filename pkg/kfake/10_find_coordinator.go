package kfake

import (
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// FindCoordinator: v0-6
//
// Supports coordinator types:
// * 0: Group coordinator
// * 1: Transaction coordinator
// * 2: Share coordinator (KIP-932)
//
// Version notes:
// * v1: CoordinatorType, ThrottleMillis
// * v3: Flexible versions
// * v4: Multiple coordinator keys in single request (KIP-699)
// * v6: Share groups (KIP-932) - coordinator type 2

func init() { regKey(10, 0, 6) }

func (c *Cluster) handleFindCoordinator(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.FindCoordinatorRequest)
	resp := req.ResponseKind().(*kmsg.FindCoordinatorResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	var unknown bool
	if req.CoordinatorType != 0 && req.CoordinatorType != 1 && req.CoordinatorType != 2 {
		unknown = true
	}
	// Share coordinator (type 2) requires FindCoordinator v6+.
	if req.CoordinatorType == 2 && req.Version < 6 {
		unknown = true
	}

	if req.Version <= 3 {
		req.CoordinatorKeys = append(req.CoordinatorKeys, req.CoordinatorKey)
		defer func() {
			resp.ErrorCode = resp.Coordinators[0].ErrorCode
			resp.ErrorMessage = resp.Coordinators[0].ErrorMessage
			resp.NodeID = resp.Coordinators[0].NodeID
			resp.Host = resp.Coordinators[0].Host
			resp.Port = resp.Coordinators[0].Port
		}()
	}

	addc := func(key string) *kmsg.FindCoordinatorResponseCoordinator {
		sc := kmsg.NewFindCoordinatorResponseCoordinator()
		sc.Key = key
		resp.Coordinators = append(resp.Coordinators, sc)
		return &resp.Coordinators[len(resp.Coordinators)-1]
	}
	// Kafka names no node on an error: node -1, port -1.
	fail := func(sc *kmsg.FindCoordinatorResponseCoordinator, e *kerr.Error) {
		sc.ErrorCode = e.Code
		sc.NodeID, sc.Port = -1, -1
		if req.Version <= 3 {
			msg := e.Description
			sc.ErrorMessage = &msg
		}
	}

	for _, key := range req.CoordinatorKeys {
		sc := addc(key)
		if unknown {
			fail(sc, kerr.InvalidRequest)
			continue
		}

		// ACL check based on coordinator type
		var (
			e          *kerr.Error
			shareKeyOK bool
		)
		switch req.CoordinatorType {
		case 0: // Group
			e = c.deny(creq, key, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe, faultKey{group: key})
		case 1: // Transaction
			e = c.deny(creq, key, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationDescribe, faultKey{txnID: key})
		case 2: // Share (KIP-932): Kafka requires CLUSTER_ACTION on the cluster.
			var sk faultKey
			sk, shareKeyOK = parseShareCoordinatorKey(key)
			e = c.denyCluster(creq, kmsg.ACLOperationClusterAction, sk)
		}
		if e != nil {
			fail(sc, e)
			continue
		}

		// Share keys must be groupId:topicId:partition; the real broker
		// validates this and rejects e.g. a bare group id (#1330).
		if req.CoordinatorType == 2 && !shareKeyOK {
			fail(sc, kerr.InvalidRequest)
			continue
		}

		b := c.coordinator(key)
		sc.NodeID = b.node
		sc.Host, sc.Port = b.hostport()
	}

	return resp, nil
}

// parseShareCoordinatorKey parses a groupId:topicId:partition share
// coordinator key as Kafka does, and returns the fault key for as much as
// parses and whether all of it is valid. Kafka splits on ":" and drops
// trailing empty tokens; the last token is a 32-bit partition, the one
// before is a base64 topic ID, and the rest is a group ID that is not
// blank.
func parseShareCoordinatorKey(key string) (faultKey, bool) {
	tokens := strings.Split(key, ":")
	for len(tokens) > 0 && tokens[len(tokens)-1] == "" {
		tokens = tokens[:len(tokens)-1]
	}
	n := len(tokens)
	if n < 3 {
		return faultKey{}, false
	}
	k := faultKey{group: strings.Join(tokens[:n-2], ":")}
	ok := strings.TrimSpace(k.group) != ""
	if id, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(tokens[n-2], "=")); err == nil && len(id) == len(k.topicID) {
		copy(k.topicID[:], id)
	} else {
		ok = false
	}
	if p, err := strconv.ParseInt(tokens[n-1], 10, 32); err == nil {
		k = k.part(int32(p))
	} else {
		ok = false
	}
	return k, ok
}
