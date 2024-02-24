module github.com/twmb/franz-go

go 1.19

require (
	github.com/klauspost/compress v1.17.4
	github.com/pierrec/lz4/v4 v4.1.19
	github.com/twmb/franz-go/pkg/kmsg v1.7.0
	golang.org/x/crypto v0.17.0
)

retract v1.11.4 // This version is actually a breaking change and requires a major version change.
