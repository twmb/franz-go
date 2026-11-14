package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// SASLAuthenticate: v0-2
//
// Supported mechanisms:
// * PLAIN
// * SCRAM-SHA-256
// * SCRAM-SHA-512
//
// Behavior:
// * A failed authentication answers SASL_AUTHENTICATION_FAILED, then we
//   close the connection
//
// Version notes:
// * v1: SessionLifetimeMs in response
// * v2: Flexible versions

func init() { regKey(36, 0, 2) }

func (c *Cluster) handleSASLAuthenticate(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.SASLAuthenticateRequest)
	resp := req.ResponseKind().(*kmsg.SASLAuthenticateResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// Kafka sends the failure, then closes the connection. A SCRAM
	// failure gets a generic message that does not say whether the user
	// exists. During re-authentication, the connection carries the
	// session expiry of its previous authentication.
	fail := func(msg string) (kmsg.Response, error) {
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		resp.ErrorMessage = &msg
		creq.closeAfter = true
		return resp, nil
	}
	failScram := func(mechanism string) (kmsg.Response, error) {
		what := "authentication"
		if creq.cc.hasSessionExpiry {
			what = "re-authentication"
		}
		return fail("Authentication failed during " + what + " due to invalid credentials with SASL mechanism " + mechanism)
	}
	// On re-authentication, the principal must not change.
	changesUser := func(u string) bool {
		return creq.cc.user != "" && creq.cc.user != u
	}
	changedUserMsg := func(u string) string {
		return "Cannot change principals during re-authentication from User:" + creq.cc.user + ": User:" + u
	}

	switch creq.cc.saslStage {
	default:
		resp.ErrorCode = kerr.IllegalSaslState.Code
		if creq.cc.saslStage == saslStageComplete {
			resp.ErrorMessage = kmsg.StringPtr("SaslAuthenticate request received after successful authentication")
		}
		return resp, nil

	case saslStageAuthPlain:
		u, p, failure := saslSplitPlain(req.SASLAuthBytes)
		if failure != "" {
			return fail(failure)
		}
		pass, ok := c.sasls.plain[u]
		switch {
		case u == "":
			return fail("Authentication failed: username not specified")
		case p == "":
			return fail("Authentication failed: password not specified")
		case !ok || p != pass:
			return fail("Authentication failed: Invalid username or password")
		case changesUser(u):
			return fail(changedUserMsg(u))
		}
		creq.cc.saslStage = saslStageComplete
		creq.cc.user = u
		creq.cc.mechanism = saslPlain
		c.maybeFillSessionLifetime(creq.cc, resp)

	case saslStageAuthScram0_256, saslStageAuthScram0_512:
		mechanism, creds := saslScram256, c.sasls.scram256
		if creq.cc.saslStage == saslStageAuthScram0_512 {
			mechanism, creds = saslScram512, c.sasls.scram512
		}
		c0, err := scramParseClient0(req.SASLAuthBytes)
		if err != nil {
			return failScram(mechanism)
		}
		a, ok := creds[c0.user]
		if !ok {
			return failScram(mechanism)
		}
		if changesUser(c0.user) {
			return fail(changedUserMsg(c0.user))
		}
		s0, serverFirst := scramServerFirst(c0, a)
		resp.SASLAuthBytes = serverFirst
		creq.cc.saslStage = saslStageAuthScram1
		creq.cc.s0 = &s0
		creq.cc.user = c0.user

	case saslStageAuthScram1:
		serverFinal, err := creq.cc.s0.serverFinal(req.SASLAuthBytes)
		if err != nil {
			return failScram(creq.cc.s0.a.mechanism)
		}
		resp.SASLAuthBytes = serverFinal
		creq.cc.saslStage = saslStageComplete
		creq.cc.mechanism = creq.cc.s0.a.mechanism
		creq.cc.s0 = nil
		c.maybeFillSessionLifetime(creq.cc, resp)
	}

	return resp, nil
}

// maybeFillSessionLifetime sets SessionLifetimeMillis on the final (successful)
// SaslAuthenticate response when re-authentication is enabled, KIP-368, and
// records on the connection whether this session carries an expiration (which
// is what gates a future re-handshake; see 17_sasl_handshake.go). Real
// brokers compute min(connections.max.reauth.ms, credential expiry); kfake
// credentials do not expire, so the config value is the lifetime. The field
// exists from v1 on; kmsg only serializes it for v1+, so setting it
// unconditionally on the response is version safe.
func (c *Cluster) maybeFillSessionLifetime(cc *clientConn, resp *kmsg.SASLAuthenticateResponse) {
	reauthMs := c.connectionsMaxReauthMs()
	if reauthMs > 0 {
		resp.SessionLifetimeMillis = reauthMs
	}
	cc.hasSessionExpiry = reauthMs > 0
}
