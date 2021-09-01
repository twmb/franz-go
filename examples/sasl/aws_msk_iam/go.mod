module aws_msk_iam

go 1.16

require (
	github.com/aws/aws-sdk-go v1.40.34
	github.com/twmb/franz-go v0.8.3
	github.com/twmb/franz-go/pkg/kmsg v0.0.0-20210901051457-3c197a133ddd
)

replace github.com/twmb/franz-go => ../../..
