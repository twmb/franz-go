module dropin_zap

go 1.16

require (
	github.com/twmb/franz-go v0.8.3
	github.com/twmb/franz-go/plugin/kzap v0.1.0
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/zap v1.19.0
)

replace github.com/twmb/franz-go => ../../..
