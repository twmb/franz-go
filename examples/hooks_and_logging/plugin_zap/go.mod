module dropin_zap

go 1.24

toolchain go1.24.2

require (
	github.com/twmb/franz-go v1.18.1
	github.com/twmb/franz-go/plugin/kzap v1.1.2
	go.uber.org/zap v1.27.0
)

require (
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)
