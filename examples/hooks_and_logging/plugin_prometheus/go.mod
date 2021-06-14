module dropin_prometheus

go 1.16

require (
	github.com/twmb/franz-go v0.8.3
	github.com/twmb/franz-go/plugin/kprom v0.1.0
)

replace github.com/twmb/franz-go => ../../..
