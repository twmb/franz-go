module dropin_prometheus

go 1.16

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/prometheus/common v0.30.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/twmb/franz-go v0.8.3
	github.com/twmb/franz-go/plugin/kprom v0.1.0
	google.golang.org/protobuf v1.27.1 // indirect
)

replace github.com/twmb/franz-go => ../../..
