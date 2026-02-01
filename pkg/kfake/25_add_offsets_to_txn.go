package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(25, 0, 3) }

func (c *Cluster) handleAddOffsetsToTxn(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.AddOffsetsToTxnRequest)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if c.pids.handleAddOffsetsToTxn(creq) {
		return nil, nil
	}
	resp := req.ResponseKind().(*kmsg.AddOffsetsToTxnResponse)
	resp.ErrorCode = kerr.UnknownServerError.Code
	return resp, nil
}
