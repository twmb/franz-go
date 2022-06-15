module manual_committing

go 1.18

require (
	github.com/twmb/franz-go v1.5.3
	github.com/twmb/franz-go/pkg/kadm v0.0.0-20211016003631-fbf9239e2698
)

require (
	github.com/klauspost/compress v1.15.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.1.0 // indirect
)

replace github.com/twmb/franz-go => ../..

replace github.com/twmb/franz-go/pkg/kadm => ../../pkg/kadm
