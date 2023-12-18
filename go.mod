module github.com/twmb/franz-go

go 1.18

require (
	github.com/klauspost/compress v1.16.7
	github.com/pierrec/lz4/v4 v4.1.18
	github.com/twmb/franz-go/pkg/kmsg v1.7.0
	golang.org/x/crypto v0.17.0
)

retract v1.11.4 // This version is actually a breaking change and requires a major version change.
