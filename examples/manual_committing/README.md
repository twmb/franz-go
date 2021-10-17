Assigning partitions manually and committing
===

If you consume outside the context of a group, but still want to use Kafka to
manage offsets, you can use the `kadm` [package](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm)
to manually commit offsets.

This contains an example that consumes records from manually assigned partitions
and then commits using kadm.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see the consumer output!

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to set the topic to consume from

`-group` can be specified to set the group to commit to
