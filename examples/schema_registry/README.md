Schema registry: registering schema, encoding, and decoding
===

This small example shows how to register a schema to the schema registry and
then use that registered schema for encoding and decoding.

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to override the default topic produced to from 'test'.

`-registry` can be specified to override the default registry url
localhost:8081.
