module testing_with_kfake

go 1.27.0

require (
	github.com/twmb/franz-go v1.22.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260218055430-fc72d8313608
)

require (
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
)

replace github.com/twmb/franz-go => ../..

replace github.com/twmb/franz-go/pkg/kfake => ../../pkg/kfake

replace github.com/twmb/franz-go/pkg/kmsg => ../../pkg/kmsg
