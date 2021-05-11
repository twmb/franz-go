Transactional producing, EOS consuming/producing
===

This contains an example that will produce 10 records in succession as a part
of a single transaction. The producer will commit the batch, and the next batch
it will abort, and this will flip indefinitely.

The EOS consumer/producer will consume only the committed produced records,
prefix them with "eos ", and then produce them to another topic.

Two flags are required: `-produce-to` and `-eos-to`.

This program outputs a bunch of debug output, but does not output anything
produced or consumed. Use `kcl` to see the final created committed offsets.

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-produce-to` specifies which topic for the input transactional producer to
produce to.

`-eos-to` specifies which topic the EOS consumer/producer will produce to.

`-group` specifies which group will be used for the EOS consumer.

`-produce-txn-id` specifies which transactional ID should be used for the
transactional producer.

`-consume-txn-id` specifies which transactional ID should be used for the EOS
consumer/producer.
