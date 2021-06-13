module dropin_zap

go 1.16

require (
	github.com/twmb/franz-go v0.8.1
	github.com/twmb/franz-go/plugin/kzap v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.17.0 // indirect
)

replace (
	github.com/twmb/franz-go => ../../..
	github.com/twmb/franz-go/plugin/kzap => ../../../plugin/kzap
)
