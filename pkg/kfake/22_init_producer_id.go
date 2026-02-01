package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// InitProducerID: v0-5
//
// Behavior:
// * Allocates producer ID and epoch for idempotent/transactional producers
// * Handles transaction ID registration for transactional producers
//
// Version notes:
// * v2: ThrottleMillis
// * v3: ProducerID and ProducerEpoch in request for existing producers
// * v4: Flexible versions
// * v5: ProducerEpoch in response for KIP-890 epoch bumping

func init() { regKey(22, 0, 5) }

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
