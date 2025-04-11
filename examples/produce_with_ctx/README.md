# Produce with ctx

This small example demonstrates a per message context with a timeout.

We _do_ expect franz-go kgo.Client to fully support context deadlines.

## Examples

```
$ docker run --rm -it --name=source -p 8081:8081 -p 9092:9092 -p 9644:9644 redpandadata/redpanda redpanda start --node-id 0 --mode dev-container --set "rpk.additional_start_flags=[--reactor-backend=epoll]" --set redpanda.auto_create_topics_enabled=true --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr host.docker.internal:9092 --schema-registry-addr 0.0.0.0:8081
```

```
$ go run .
$ rpk topic consume foobar -o ':end' -X brokers=localhost:9092
{
  "topic": "foobar",
  "key": "0",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 0,
  "partition": 0,
  "offset": 0
}
{
  "topic": "foobar",
  "key": "1",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 1,
  "partition": 0,
  "offset": 1
}
{
  "topic": "foobar",
  "key": "2",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 2,
  "partition": 0,
  "offset": 2
}
{
  "topic": "foobar",
  "key": "3",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 3,
  "partition": 0,
  "offset": 3
}
{
  "topic": "foobar",
  "key": "4",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 4,
  "partition": 0,
  "offset": 4
}
{
  "topic": "foobar",
  "key": "5",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 5,
  "partition": 0,
  "offset": 5
}
{
  "topic": "foobar",
  "key": "6",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 6,
  "partition": 0,
  "offset": 6
}
{
  "topic": "foobar",
  "key": "7",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 7,
  "partition": 0,
  "offset": 7
}
{
  "topic": "foobar",
  "key": "8",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 8,
  "partition": 0,
  "offset": 8
}
{
  "topic": "foobar",
  "key": "9",
  "value": "{\"test\":\"foo\"}",
  "timestamp": 9,
  "partition": 0,
  "offset": 9
}
```