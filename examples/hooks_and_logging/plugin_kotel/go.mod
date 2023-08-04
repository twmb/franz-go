module kotel_hooks

go 1.20

require (
	github.com/google/uuid v1.3.0
	github.com/twmb/franz-go v1.14.3
	github.com/twmb/franz-go/plugin/kotel v1.4.0
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v0.39.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/sdk/metric v0.39.0
	go.opentelemetry.io/otel/trace v1.16.0
)

require (
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.6.1 // indirect
	go.opentelemetry.io/otel/metric v1.16.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
)
