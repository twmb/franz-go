module bench

go 1.18

require (
	github.com/twmb/franz-go v1.5.3
	github.com/twmb/franz-go/plugin/kprom v0.3.0
	github.com/twmb/tlscfg v1.2.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/klauspost/compress v1.15.6 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	github.com/prometheus/client_golang v1.12.2 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.34.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.1.0 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/sys v0.0.0-20220614162138-6c1b26c55098 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
)

replace (
	github.com/twmb/franz-go => ../..
	github.com/twmb/franz-go/pkg/kmsg => ../../pkg/kmsg
	github.com/twmb/franz-go/plugin/kprom => ../../plugin/kprom
)
