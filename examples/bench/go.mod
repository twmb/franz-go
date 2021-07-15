module bench

go 1.16

require (
	github.com/twmb/franz-go v0.8.7
	github.com/twmb/franz-go/plugin/kprom v0.3.0
)

replace (
	github.com/twmb/franz-go => ../..
	github.com/twmb/franz-go/plugin/kprom => ../../plugin/kprom
)
