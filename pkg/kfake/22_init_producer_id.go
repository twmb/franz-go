package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(22, 0, 4) }

func (c *Cluster) handleInitProducerID(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.InitProducerIDRequest)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if c.pids.handleInitProducerID(creq) {
		return nil, nil
	}
	resp := req.ResponseKind().(*kmsg.InitProducerIDResponse)
	resp.ErrorCode = kerr.UnknownServerError.Code
	return resp, nil
}
