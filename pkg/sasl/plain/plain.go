// Package plain provides PLAIN sasl authentication as specified in RFC4616.
package plain

import (
	"context"
	"errors"

	"github.com/twmb/kafka-go/pkg/sasl"
)

// New returns a sasl authenticator that will call userpassFn whenever sasl
// authentication is needed. The function must return the username and the
// password to use for authentication. The username and password is used for a
// single session.
func New(userpassFn func(context.Context) (string, string, error)) sasl.Authenticator {
	return plain(userpassFn)
}

type plain func(context.Context) (string, string, error)

func (plain) Name() string { return "PLAIN" }
func (fn plain) Authenticate(ctx context.Context) (sasl.Session, []byte, error) {
	user, pass, err := fn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return session{}, []byte("\x00" + user + "\x00" + pass), nil
}

type session struct{}

func (session) Challenge(resp []byte) (bool, []byte, error) {
	if len(resp) != 0 {
		return false, nil, errors.New("unexpected data in plain response")
	}
	return true, nil, nil
}
