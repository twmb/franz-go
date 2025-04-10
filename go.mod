module github.com/twmb/franz-go

go 1.23.0

toolchain go1.24.1

require (
	github.com/klauspost/compress v1.18.0
	github.com/pierrec/lz4/v4 v4.1.22
	github.com/twmb/franz-go/pkg/kmsg v1.11.0
	golang.org/x/crypto v0.37.0
)

retract v1.11.4 // This version is actually a breaking change and requires a major version change.
