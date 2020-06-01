// Package plain provides PLAIN sasl authentication as specified in RFC4616.
package plain

import (
	"context"
	"errors"

	"github.com/twmb/kafka-go/pkg/sasl"
)

// Auth contains information for authentication.
type Auth struct {
	// Zid is an optional authorization ID to use in authenticating.
	Zid string

	// User is username to use for authentication.
	User string

	// Pass is the password to use for authentication.
	Pass string

	_internal struct{} // require explicit field initalization
}

// Plain returns a sasl mechanism that will call authFn whenever sasl
// authentication is needed. The returned Auth is used for a single session.
func Plain(authFn func(context.Context) (Auth, error)) sasl.Mechanism {
	return plain(authFn)
}

type plain func(context.Context) (Auth, error)

func (plain) Name() string { return "PLAIN" }
func (fn plain) Authenticate(ctx context.Context) (sasl.Session, []byte, error) {
	auth, err := fn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return session{}, []byte(auth.Zid + "\x00" + auth.User + "\x00" + auth.Pass), nil
}

type session struct{}

func (session) Challenge(resp []byte) (bool, []byte, error) {
	if len(resp) != 0 {
		return false, nil, errors.New("unexpected data in plain response")
	}
	return true, nil, nil
}
