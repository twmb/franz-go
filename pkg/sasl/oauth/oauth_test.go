package oauth

import (
	"context"
	"testing"
)

// The first client message carries the zid, the token, and the extensions. We
// escape or refuse a value that could end its own field and start a new field.
func TestAuthenticate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		auth    Auth
		want    string // the message, unless wantErr
		wantErr bool
	}{
		{name: "zid is escaped", auth: Auth{Zid: "a=b,c", Token: "t", Extensions: map[string]string{"k": "v"}}, want: "n,a=a=3Db=2Cc,\x01auth=Bearer t\x01k=v\x01\x01"},
		{name: "token with \\x01", auth: Auth{Token: "t\x01k=v"}, wantErr: true},
		{name: "zid with \\x01", auth: Auth{Zid: "z\x01k=v", Token: "t"}, wantErr: true},
		{name: "extension named auth", auth: Auth{Token: "t", Extensions: map[string]string{"auth": "Bearer u"}}, wantErr: true},
		{name: "extension key with =", auth: Auth{Token: "t", Extensions: map[string]string{"k=": "v"}}, wantErr: true},
		{name: "extension value with \\x01", auth: Auth{Token: "t", Extensions: map[string]string{"k": "v\x01auth=Bearer u"}}, wantErr: true},
	} {
		_, msg, err := tc.auth.AsMechanism().Authenticate(context.Background(), "")
		if (err != nil) != tc.wantErr || string(msg) != tc.want {
			t.Errorf("%s: got %q, err %v; want %q, err %v", tc.name, msg, err, tc.want, tc.wantErr)
		}
	}
}
