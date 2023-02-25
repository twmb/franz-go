module github.com/twmb/franz-go

go 1.18

require (
	github.com/klauspost/compress v1.15.15
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/twmb/franz-go/pkg/kmsg v1.4.0
	golang.org/x/crypto v0.6.0
)

retract v1.11.4 // This version is actually a breaking change and requires a major version change.
