package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

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
