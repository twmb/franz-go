package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AddPartitionsToTxn: v0-5
//
// Behavior:
// * Registers partitions as part of an ongoing transaction
// * Must be called before producing to partitions (pre-KIP-890 clients)
// * KIP-890 clients (v4+) can skip this and use implicit partition addition
//
// Version notes:
// * v3: Flexible versions
// * v4: Batched transactions support (KIP-890)
// * v5: Epoch bumping support (KIP-890)

func init() { regKey(24, 0, 5) }

func (c *Cluster) handleAddPartitionsToTxn(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.AddPartitionsToTxnRequest)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if c.pids.handleAddPartitionsToTxn(creq) {
		return nil, nil
	}
	resp := req.ResponseKind().(*kmsg.AddPartitionsToTxnResponse)
	resp.ErrorCode = kerr.UnknownServerError.Code
	return resp, nil
}
