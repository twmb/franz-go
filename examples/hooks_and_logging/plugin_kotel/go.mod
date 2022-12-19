module kotel_hooks

go 1.19

require (
	github.com/google/uuid v1.3.0
	github.com/twmb/franz-go v1.10.4
	github.com/twmb/franz-go/plugin/kotel v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.11.2
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v0.34.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.11.2
	go.opentelemetry.io/otel/sdk v1.11.2
	go.opentelemetry.io/otel/sdk/metric v0.34.0
	go.opentelemetry.io/otel/trace v1.11.2
)

require (
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.15.13 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.2.0 // indirect
	go.opentelemetry.io/otel/metric v0.34.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
)

replace github.com/twmb/franz-go => ../../..

replace github.com/twmb/franz-go/plugin/kotel => ../../../plugin/kotel
