module github.com/twmb/franz-go/pkg/kmsg

go 1.18

retract (
	v2.0.1 // This forced a breaking change in kgo, which is not wanted at the moment.
	v2.0.0 // This forced a breaking change in kgo, which is not wanted at the moment.
)
