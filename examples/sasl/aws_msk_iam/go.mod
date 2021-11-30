module aws_msk_iam

go 1.16

require (
	github.com/aws/aws-sdk-go v1.40.42
	github.com/twmb/franz-go v0.8.3
	github.com/twmb/franz-go/pkg/kmsg v0.0.0-20211127185622-3b34db0c6d1e
)

replace github.com/twmb/franz-go => ../../..
