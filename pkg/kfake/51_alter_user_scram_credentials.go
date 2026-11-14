package kfake

import (
	"bytes"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AlterUserSCRAMCredentials: v0
//
// Behavior:
// * Must be sent to the controller
// * Creates, updates, or deletes SCRAM credentials for users
// * Supports SCRAM-SHA-256 (mechanism 1) and SCRAM-SHA-512 (mechanism 2)
// * Iterations must be between 4096 and 16384
// * One answer per user: a second change to a user fails the user with
//   DUPLICATE_RESOURCE and changes nothing for it

func init() { regKey(51, 0, 0) }

func (c *Cluster) handleAlterUserSCRAMCredentials(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.AlterUserSCRAMCredentialsRequest)
		resp = req.ResponseKind().(*kmsg.AlterUserSCRAMCredentialsResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// We validate as Kafka does: a user's first failure sticks and skips
	// the user's later entries, and a second entry for a user with a
	// valid pending change is DUPLICATE_RESOURCE and drops that change.
	var (
		order []string // users in request order
		codes = map[string]int16{}
		dels  = map[string]kmsg.AlterUserSCRAMCredentialsRequestDeletion{}
		ups   = map[string]kmsg.AlterUserSCRAMCredentialsRequestUpsertion{}
	)
	seen := func(u string) bool {
		code, ok := codes[u]
		switch {
		case !ok:
			order = append(order, u)
			return false
		case code == 0:
			codes[u] = kerr.DuplicateResource.Code
		}
		return true
	}
	for _, d := range req.Deletions {
		if seen(d.Name) {
			continue
		}
		switch {
		case d.Name == "":
			codes[d.Name] = kerr.UnacceptableCredential.Code
		case d.Mechanism != 1 && d.Mechanism != 2:
			codes[d.Name] = kerr.UnsupportedSaslMechanism.Code
		default:
			if _, ok := c.sasls.scramMap(d.Mechanism)[d.Name]; !ok {
				codes[d.Name] = kerr.ResourceNotFound.Code
			} else {
				codes[d.Name] = 0
				dels[d.Name] = d
			}
		}
	}
	for _, u := range req.Upsertions {
		if seen(u.Name) {
			continue
		}
		switch {
		case u.Name == "":
			codes[u.Name] = kerr.UnacceptableCredential.Code
		case u.Mechanism != 1 && u.Mechanism != 2:
			codes[u.Name] = kerr.UnsupportedSaslMechanism.Code
		case u.Iterations < 4096 || u.Iterations > 16384: // Kafka min/max
			codes[u.Name] = kerr.UnacceptableCredential.Code
		default:
			codes[u.Name] = 0
			ups[u.Name] = u
		}
	}

	addr := func(u string) *kmsg.AlterUserSCRAMCredentialsResponseResult {
		sr := kmsg.NewAlterUserSCRAMCredentialsResponseResult()
		sr.User = u
		resp.Results = append(resp.Results, sr)
		return &resp.Results[len(resp.Results)-1]
	}
	// A fault can answer a user before the work runs. The work's own answer
	// for that user must not add an entry or replace the code.
	answered := make(map[string]bool)
	doneu := func(u string, code int16) {
		if answered[u] {
			return
		}
		answered[u] = true
		addr(u).ErrorCode = code
	}

	// Faults fire only on the controller; elsewhere we answer
	// NOT_CONTROLLER below.
	clusterFault := c.denyCluster(creq, kmsg.ACLOperationAlter, faultKey{misrouted: b != c.controller})
	if e := clusterFault; e != nil && creq.skipsWork(e) {
		for _, u := range order {
			doneu(u, e.Code)
		}
		return resp, nil
	}
	if b != c.controller {
		for _, u := range order {
			doneu(u, kerr.NotController.Code)
		}
		return resp, nil
	}

	// A timed-out change still changes the credential, and its user is
	// answered with the fault.
	for _, u := range order {
		e := clusterFault
		if e == nil {
			e = creq.faults.check(faultKey{resource: u})
		}
		if e == nil {
			continue
		}
		doneu(u, e.Code)
		if creq.skipsWork(e) {
			codes[u] = e.Code
		}
	}

	for _, u := range order {
		if codes[u] != 0 {
			doneu(u, codes[u])
			continue
		}
		if d, ok := dels[u]; ok {
			delete(c.sasls.scramMap(d.Mechanism), u)
			doneu(u, 0)
			continue
		}
		up := ups[u]
		m := &c.sasls.scram256
		mech := saslScram256
		if up.Mechanism == 2 {
			m = &c.sasls.scram512
			mech = saslScram512
		}
		if *m == nil {
			*m = make(map[string]scramAuth)
		}
		(*m)[u] = scramAuth{
			mechanism:  mech,
			iterations: int(up.Iterations),
			saltedPass: bytes.Clone(up.SaltedPassword),
			salt:       bytes.Clone(up.Salt),
		}
		doneu(u, 0)
	}

	c.persistSASLState()
	return resp, nil
}

// scramMap returns the credentials for a SCRAM mechanism, 1 for SHA-256 and
// 2 for SHA-512.
func (s *sasls) scramMap(mechanism int8) map[string]scramAuth {
	if mechanism == 2 {
		return s.scram512
	}
	return s.scram256
}
