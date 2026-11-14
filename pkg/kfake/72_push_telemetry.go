package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// PushTelemetry: v0
//
// KIP-714: Client metrics and observability.
//
// Behavior:
// * No ACL checks (telemetry uses subscription-based validation)
// * Validates ClientInstanceID is not zero/reserved
// * Validates SubscriptionID against the client's current subscription
// * Validates the compression type and payload size
// * Accepts and discards metrics (no actual processing)
// * Refuses a push after a push with the Terminating flag
//
// Version notes:
// * v0: Initial version

func init() { regKey(72, 0, 0) }

func (c *Cluster) handlePushTelemetry(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.PushTelemetryRequest)
	resp := req.ResponseKind().(*kmsg.PushTelemetryResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if req.ClientInstanceID == [16]byte{} || req.ClientInstanceID == telemReservedID {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	// Like Kafka, a push from a client we have not seen, such as one
	// whose Get went to a broker before a restart, is checked against
	// the subscription we would answer it now. A client that said it is
	// terminating may push nothing more.
	tc := c.telemClientFor(creq, req.ClientInstanceID)
	if tc.terminating {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}
	if _, _, subID := c.telemSubscription(tc); subID != req.SubscriptionID {
		resp.ErrorCode = kerr.UnknownSubscriptionID.Code
		return resp, nil
	}
	if req.CompressionType < 0 || req.CompressionType > 4 {
		resp.ErrorCode = kerr.UnsupportedCompressionType.Code
		return resp, nil
	}
	if len(req.Metrics) > telemMaxBytes {
		resp.ErrorCode = kerr.TelemetryTooLarge.Code
		return resp, nil
	}

	if req.Terminating {
		tc.terminating = true
		c.telem[req.ClientInstanceID] = tc
	}

	return resp, nil
}
