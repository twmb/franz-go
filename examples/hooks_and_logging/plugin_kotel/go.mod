module kotel_hooks

go 1.22

toolchain go1.22.0

require (
	github.com/google/uuid v1.6.0
	github.com/twmb/franz-go v1.16.1
	github.com/twmb/franz-go/plugin/kotel v1.4.1
	go.opentelemetry.io/otel v1.26.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.26.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.26.0
	go.opentelemetry.io/otel/sdk v1.26.0
	go.opentelemetry.io/otel/sdk/metric v1.26.0
	go.opentelemetry.io/otel/trace v1.26.0
)

require (
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.8.0 // indirect
	go.opentelemetry.io/otel/metric v1.26.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
)
