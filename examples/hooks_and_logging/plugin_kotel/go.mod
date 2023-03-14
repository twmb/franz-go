module kotel_hooks

go 1.20

require (
	github.com/google/uuid v1.3.0
	github.com/twmb/franz-go v1.13.0
	github.com/twmb/franz-go/plugin/kotel v1.0.1
	go.opentelemetry.io/otel v1.14.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v0.37.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.14.0
	go.opentelemetry.io/otel/sdk v1.14.0
	go.opentelemetry.io/otel/sdk/metric v0.37.0
	go.opentelemetry.io/otel/trace v1.14.0
)

require (
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.16.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.4.0 // indirect
	go.opentelemetry.io/otel/metric v0.37.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
)
