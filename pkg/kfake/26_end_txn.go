package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// EndTxn: v0-5
//
// Behavior:
// * Commits or aborts an ongoing transaction
// * Marks batches as committed/aborted, writes control batch
// * Applies staged offset commits on commit, discards on abort
// * Recalculates LSO for read_committed consumers
//
// Version notes:
// * v2: ThrottleMillis
// * v3: Flexible versions
// * v5: Returns new ProducerEpoch for KIP-890 epoch bumping

func init() { regKey(26, 0, 5) }

func (c *Cluster) handleEndTxn(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.EndTxnRequest)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if c.pids.handleEndTxn(creq) {
		return nil, nil
	}
	resp := req.ResponseKind().(*kmsg.EndTxnResponse)
	resp.ErrorCode = kerr.InvalidTxnState.Code
	return resp, nil
}
