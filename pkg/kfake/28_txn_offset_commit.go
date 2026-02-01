package kfake

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TxnOffsetCommit: v0-5
//
// Behavior:
// * Stages offset commits as part of a transaction
// * Offsets are applied on EndTxn commit, discarded on abort
// * v3+: GenerationID/MemberID validation for zombie fencing (KIP-447)
// * v5+: Implicit group addition to transaction (KIP-890)
//
// Version notes:
// * v2: ThrottleMillis
// * v3: Flexible versions, GenerationID and MemberID for zombie fencing (KIP-447)
// * v4-5: No field changes; v5 enables implicit group addition (KIP-890)

func init() { regKey(28, 0, 5) }

func (c *Cluster) handleTxnOffsetCommit(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.TxnOffsetCommitRequest)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if c.pids.handleTxnOffsetCommit(creq) {
		return nil, nil
	}
	resp := req.ResponseKind().(*kmsg.TxnOffsetCommitResponse) // TODO CLAUDE add primitive error returning for all partitions. likely invalid group id?
	return resp, nil
}
