module github.com/twmb/franz-go/pkg/sr/serde/protobuf

go 1.26.0

// RELEASE ORDER: this module needs the serde package, which first appears in
// pkg/sr v1.9.0. Tag pkg/sr/v1.9.0 and bump the require below before tagging
// this module, otherwise `go get` of this module resolves a pkg/sr without
// serde. Until then the lint and test jobs build it through a go.work.
require (
	github.com/jhump/protoreflect v1.17.0
	github.com/twmb/franz-go/pkg/sr v1.8.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	golang.org/x/sync v0.16.0 // indirect
)
