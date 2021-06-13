module prometheus_hooks

go 1.16

require (
	github.com/prometheus/client_golang v1.10.0
	github.com/twmb/franz-go v0.8.1
)

replace github.com/twmb/franz-go => ../../..
