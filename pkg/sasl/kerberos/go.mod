module github.com/twmb/franz-go/pkg/sasl/kerberos

go 1.15

replace github.com/twmb/franz-go => ../../..

require (
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2
	github.com/twmb/franz-go v1.5.0
	golang.org/x/net v0.0.0-20220425223048-2871e0cb64e4 // indirect
)
