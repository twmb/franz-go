module dropin_prometheus

go 1.16

require (
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/twmb/franz-go v0.8.1
	github.com/twmb/franz-go/plugin/kgmetrics v0.0.0-00010101000000-000000000000
)

replace (
	github.com/twmb/franz-go => ../../..
	github.com/twmb/franz-go/plugin/kgmetrics => ../../../plugin/kgmetrics
)
