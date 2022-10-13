module kotel_hooks

go 1.19

require (
	github.com/google/uuid v1.1.2
	github.com/twmb/franz-go v1.8.0
	github.com/twmb/franz-go/plugin/kotel v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.11.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.32.3
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v0.32.3
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.11.0
	go.opentelemetry.io/otel/sdk v1.11.0
	go.opentelemetry.io/otel/sdk/metric v0.32.3
	go.opentelemetry.io/otel/trace v1.11.0
	google.golang.org/grpc v1.50.0
)

require (
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.2.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.11.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.32.3 // indirect
	go.opentelemetry.io/otel/metric v0.32.3 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	golang.org/x/crypto v0.0.0-20221012134737-56aed061732a // indirect
	golang.org/x/net v0.0.0-20221012135044-0b7e1fb9d458 // indirect
	golang.org/x/sys v0.0.0-20221010170243-090e33056c14 // indirect
	golang.org/x/text v0.3.8 // indirect
	google.golang.org/genproto v0.0.0-20221010155953-15ba04fc1c0e // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

replace github.com/twmb/franz-go => ../../..

replace github.com/twmb/franz-go/plugin/kotel => ../../../plugin/kotel
