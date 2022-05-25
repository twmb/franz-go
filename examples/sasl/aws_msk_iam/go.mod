module aws_msk_iam

go 1.18

require (
	github.com/aws/aws-sdk-go v1.44.22
	github.com/twmb/franz-go v0.8.3
	github.com/twmb/franz-go/pkg/kmsg v1.1.0
)

require (
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.15.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
)

replace github.com/twmb/franz-go => ../../..
