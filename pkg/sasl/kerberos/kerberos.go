// Package scram provides Kerberos v5 sasl authentication.
package kerberos

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"

	"github.com/twmb/kafka-go/pkg/sasl"
)

// Auth contains a Kerberos client and the service name that we will use to get
// a ticket for.
type Auth struct {
	// Client is a Kerberos client.
	Client *client.Client

	// Service is the service name we will get a ticket for.
	Service string
}

// Kerberos returns a sasl mechanism that will call authFn whenever sasl
// authentication is needed. The returned Auth is used for a single session.
//
// Note that it is valid to reuse a Kerberos client in the returned Auth. This
// package attaches a finalizer to the returned client that will call
// client.Destroy once the client is no longer reachable (i.e. up for garbage
// collection). Thus, if you always return a new client, this will always call
// Destroy appropriately, but if you always return a forever cached client,
// this may never call Destroy and will instead potentially be able to reuse
// ticket granting tickets. Also note that because we are attaching our own
// finalizer, this means if you attach your own, it will be removed.
func Kerberos(authFn func(context.Context) (Auth, error)) sasl.Mechanism {
	return k(authFn)
}

type k func(context.Context) (Auth, error)

var finalizersMu sync.Mutex

func (k) Name() string { return "GSSAPI" }
func (k k) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	auther, err := k(ctx)
	if err != nil {
		return nil, nil, err
	}
	c := auther.Client
	if _, err := c.IsConfigured(); err != nil {
		return nil, nil, err
	}

	finalizersMu.Lock()
	runtime.SetFinalizer(c, nil) // first clear to avoid panic if already set
	runtime.SetFinalizer(c, func(c *client.Client) { c.Destroy() })
	finalizersMu.Unlock()

	if err = c.AffirmLogin(); err != nil {
		return nil, nil, err
	}
	if strings.IndexByte(host, ':') != 0 {
		if host, _, err = net.SplitHostPort(host); err != nil {
			return nil, nil, err
		}
	}

	ticket, encKey, err := c.GetServiceTicket(auther.Service + "/" + host)
	if err != nil {
		return nil, nil, err
	}

	/*
	 * Construct Kerberos AP_REQ package, conforming to RFC-4120
	 * https://tools.ietf.org/html/rfc4120#page-84
	 */
	auth, err := types.NewAuthenticator(c.Credentials.Domain(), c.Credentials.CName())
	if err != nil {
		return nil, nil, err
	}
	auth.Cksum = types.Checksum{
		CksumType: 32771,                        // GSSAPI checksum type
		Checksum:  []byte{0: 16, 20: 48, 23: 0}, // ContextFlagInteg | ContextFlagConf
	}
	aprReq, err := messages.NewAPReq(ticket, encKey, auth)
	if err != nil {
		return nil, nil, err
	}
	arpReqMarshaled, err := aprReq.Marshal()
	if err != nil {
		return nil, nil, err
	}
	apr := append([]byte{1, 0}, arpReqMarshaled...)

	/*
	 *	Append the GSS-API header to the payload, conforming to RFC-2743
	 *	Section 3.1, Mechanism-Independent Token Format
	 *
	 *	https://tools.ietf.org/html/rfc2743#page-81
	 *
	 *	GSSAPIHeader + <specific mechanism payload>
	 */
	oid := []byte{6, 9, 42, 134, 72, 134, 247, 18, 1, 2, 2} // asn1 marshalled gssapi OID for KRB5
	gssHeader := append([]byte{0x60}, asn1LengthBytes(len(oid)+len(apr))...)
	gssHeader = append(gssHeader, oid...)

	return &session{0, c, encKey}, append(gssHeader, apr...), nil
}

type session struct {
	step   int
	client *client.Client
	encKey types.EncryptionKey
}

func (s *session) Challenge(resp []byte) (bool, []byte, error) {
	step := s.step
	s.step++
	switch step {
	case 0:
		var challenge gssapi.WrapToken
		if err := challenge.Unmarshal(resp, true); err != nil {
			return false, nil, err
		}
		isValid, err := challenge.Verify(s.encKey, 22) // 22 == GSSAPI ACCEPTOR SEAL
		if !isValid {
			return false, nil, err
		}
		response, err := gssapi.NewInitiatorWrapToken(challenge.Payload, s.encKey)
		if err != nil {
			return false, nil, err
		}
		marshalled, err := response.Marshal()
		return true, marshalled, err // we are done, but we have one more response to write ourselves
	default:
		return false, nil, fmt.Errorf("challenge / response should be done, but still going at %d", step)
	}
}

/*
RFC 2743 § 3.1:

   2a. If the indicated value is less than 128, it shall be
   represented in a single octet with bit 8 (high order) set to
   "0" and the remaining bits representing the value.

   2b. If the indicated value is 128 or more, it shall be
   represented in two or more octets, with bit 8 of the first
   octet set to "1" and the remaining bits of the first octet
   specifying the number of additional octets.  The subsequent
   octets carry the value, 8 bits per octet, most significant
   digit first.  The minimum number of octets shall be used to
   encode the length (i.e., no octets representing leading zeros
   shall be included within the length encoding).
*/
func asn1LengthBytes(l int) []byte {
	if l <= 127 {
		return []byte{byte(l)}
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(l))
	for i, v := range buf {
		if v == 0 { // skip leading zeroes
			continue
		}
		return append([]byte{128 + byte(len(buf[i:])) - 1}, buf[i:]...) // first bit 1 + number of additional bytes, remaining payload
	}
	return nil // unreachable
}
