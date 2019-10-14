// Package oauth provides OAUTHBEARER sasl authentication as specified in
// RFC7628.
package oauth

import (
	"context"
	"errors"
	"sort"

	"github.com/twmb/kafka-go/pkg/sasl"
)

// Auth contains information for authentication.
//
// This client reserves the right to add fields to this struct in the future
// if Kafka adds more capabilities to Oauth.
type Auth struct {
	// Token is the oauthbearer token to use for a single session's
	// authentication.
	Token string
	// Extensions are key value pairs to add to the authentication request.
	Extensions map[string]string

	_internal struct{} // require explicit field initalization
}

// New returns an OAUTHBEARER sasl authenticator that will call authFn whenever
// authentication is needed. The returned Auth is used for a single session.
func New(authFn func(context.Context) (Auth, error)) sasl.Authenticator {
	return oauth(authFn)
}

type oauth func(context.Context) (Auth, error)

func (oauth) Name() string { return "OAUTHBEARER" }
func (fn oauth) Authenticate(ctx context.Context) (sasl.Session, []byte, error) {
	auth, err := fn(ctx)
	if err != nil {
		return nil, nil, err
	}

	// We sort extensions for consistency, but it is not required.
	type kv struct {
		k string
		v string
	}
	kvs := make([]kv, 0, len(auth.Extensions))
	for k, v := range auth.Extensions {
		if len(k) == 0 {
			continue
		}
		kvs = append(kvs, kv{k, v})
	}
	sort.Slice(len(kvs), func(i, j int) bool { return kvs[i].k < kvs[j].k })

	// https://tools.ietf.org/html/rfc7628#section-3.1
	init := []byte("n,,\x01auth=Bearer ")
	init = append(init, auth.Token...)
	init = append(init, '\x01')
	for _, kv := range kvs {
		init = append(init, kv.k...)
		init = append(init, '=')
		init = append(init, kv.v...)
		init = append(init, '\x01')
	}
	init = append(init, '\x01')

	return session{}, init, nil
}

type session struct{}

func (session) Challenge(resp []byte) (bool, []byte, error) {
	if len(resp) != 0 {
		return false, nil, errors.New("unexpected data in oauth response")
	}
	return true, nil, nil
}
