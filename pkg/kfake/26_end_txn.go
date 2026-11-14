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
// * v2: PRODUCER_FENCED; older versions get INVALID_PRODUCER_EPOCH instead
// * v3: Flexible versions
// * v4: TRANSACTION_ABORTABLE (KIP-890)
// * v5: Returns new ProducerID and ProducerEpoch for KIP-890 epoch bumping

func init() { regKey(26, 0, 5) }

func (c *Cluster) handleEndTxn(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.EndTxnRequest)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// ACL check: WRITE on TxnID. Faults fire only on the transaction
	// coordinator; elsewhere doEnd answers NOT_COORDINATOR.
	if e := c.deny(creq, req.TransactionalID, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationWrite, faultKey{txnID: req.TransactionalID, misrouted: !c.isCoordinator(creq, req.TransactionalID)}); e != nil {
		resp := req.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = e.Code
		return resp, nil
	}

	resp := c.pids.doEnd(creq).(*kmsg.EndTxnResponse)
	if req.Version < 2 && resp.ErrorCode == kerr.ProducerFenced.Code {
		resp.ErrorCode = kerr.InvalidProducerEpoch.Code
	}
	return resp, nil
}
