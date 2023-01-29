module github.com/twmb/franz-go

go 1.15

require (
	github.com/klauspost/compress v1.15.9
	github.com/pierrec/lz4/v4 v4.1.15
	github.com/twmb/franz-go/pkg/kmsg v1.3.0
	golang.org/x/crypto v0.0.0-20220817201139-bc19a97f63c8
)

retract v1.11.4 // This version is actually a breaking change and requires a major version change.
