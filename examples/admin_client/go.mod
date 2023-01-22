module admin_client

go 1.19

require (
	github.com/twmb/franz-go v1.10.4
	github.com/twmb/franz-go/pkg/kadm v1.6.0
)

require (
	github.com/klauspost/compress v1.15.12 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.2.0 // indirect
	golang.org/x/crypto v0.3.0 // indirect
)

replace github.com/twmb/franz-go => ../..

replace github.com/twmb/franz-go/pkg/kadm => ../../pkg/kadm
