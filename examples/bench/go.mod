module bench

go 1.25.0

require (
	github.com/twmb/franz-go v1.18.1
	github.com/twmb/franz-go/plugin/kprom v1.2.1
	github.com/twmb/tlscfg v1.2.1
)

replace github.com/twmb/franz-go => ../..

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.1 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.12.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/crypto v0.43.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
