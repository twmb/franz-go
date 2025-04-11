module github.com/twmb/franz-go/pkg/kmsg

go 1.21

toolchain go1.22.0

retract (
	v1.11.0 // This version erroneously always encoded tagged uuid fields, which failed on any Kafka version that did not support the field
	v1.10.0 // This version failed to compile; I must have accidentally git-broke my fixed commit before pushing & merging
)
