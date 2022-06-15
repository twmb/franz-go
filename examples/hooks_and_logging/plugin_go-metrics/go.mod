module dropin_go_metrics

go 1.18

require (
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/twmb/franz-go v0.8.3
	github.com/twmb/franz-go/plugin/kgmetrics v0.1.0
)

require (
	github.com/klauspost/compress v1.15.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.1.0 // indirect
)

replace github.com/twmb/franz-go => ../../..
