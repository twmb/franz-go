module dropin_prometheus

go 1.16

require (
	github.com/twmb/franz-go v0.8.1
	github.com/twmb/franz-go/plugin/kprom v0.0.0-00010101000000-000000000000
)

replace (
	github.com/twmb/franz-go => ../../..
	github.com/twmb/franz-go/plugin/kprom => ../../../plugin/kprom
)
