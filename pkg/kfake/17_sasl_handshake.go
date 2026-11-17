package kfake

import (
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// SASLHandshake: v1
//
// Supported mechanisms:
// * PLAIN
// * SCRAM-SHA-256
// * SCRAM-SHA-512
//
// Note: v0 is not supported (v0 uses implicit auth after handshake)

func init() { regKey(17, 1, 1) }

func (c *Cluster) handleSASLHandshake(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.SASLHandshakeRequest)
	resp := req.ResponseKind().(*kmsg.SASLHandshakeResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if creq.cc.saslStage != saslStageBegin {
		// KIP-368: when the connection's session carries an expiration
		// (its authenticate ran while connections.max.reauth.ms was
		// positive), a new handshake on the authenticated connection
		// begins re-authentication
		// (KafkaChannel.maybeBeginServerReauthentication). Otherwise
		// the handshake is processed normally, which fails here with
		// an illegal state.
		if creq.cc.saslStage != saslStageComplete || !creq.cc.hasSessionExpiry {
			resp.ErrorCode = kerr.IllegalSaslState.Code
			return resp, nil
		}
		// Kafka begins at most one re-authentication per second on a
		// connection; a faster one is processed normally and fails.
		now := time.Now()
		if !creq.cc.lastReauth.IsZero() && now.Sub(creq.cc.lastReauth) < time.Second {
			resp.ErrorCode = kerr.IllegalSaslState.Code
			return resp, nil
		}
		creq.cc.lastReauth = now
		// Kafka answers a re-authentication that changes the mechanism
		// as if it took it, then closes the connection on the next
		// request.
		switch req.Mechanism {
		case creq.cc.mechanism:
		case saslPlain, saslScram256, saslScram512:
			creq.cc.saslStage = saslStageFailed
			return resp, nil
		}
	}

	resp.SupportedMechanisms = []string{saslPlain, saslScram256, saslScram512}
	switch req.Mechanism {
	case saslPlain:
		creq.cc.saslStage = saslStageAuthPlain
	case saslScram256:
		creq.cc.saslStage = saslStageAuthScram0_256
	case saslScram512:
		creq.cc.saslStage = saslStageAuthScram0_512
	default:
		// Kafka closes the connection after this response, so you must
		// dial again to try another mechanism.
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
		creq.cc.saslStage = saslStageFailed
	}
	return resp, nil
}
