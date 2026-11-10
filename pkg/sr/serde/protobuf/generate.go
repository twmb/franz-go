package protobuf

//go:generate protoc --go_out=. --go_opt=module=github.com/twmb/franz-go/pkg/sr/serde/protobuf -I testdata testdata/widget.proto testdata/nested.proto testdata/dep.proto testdata/with_dep.proto testdata/leaf.proto testdata/mid.proto testdata/chain.proto
