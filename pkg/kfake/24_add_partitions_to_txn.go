package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

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
