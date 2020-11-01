# Producer

## Record Reliability

By default, kgo uses idempotent production. This is automatic and cannot
(currently) be disabled. As well, the default is to always retry records
forever, and to have acks from all in sync replicas. Thus, the default is
highly reliable.

The required acks can be dropped, as can the max record retries, but this
is not recommended.

## Exactly Once Semantics

As mentioned above, kgo supports EOS. Because there are a lot of corner cases
around transactions, this client favors a "if we maybe should abort, abort"
approach. This client provides a `GroupTransactSession` type that is used
to manage consume-modify-produce transactions. Any time it is possible that
the transaction should be aborted, the session sets its internal abort state.
This may mean you will end up re-processing records more than necessary, but
in general this should only happen on group rebalances, which should be rare.

By proxy, producer-only transactions are also supported. This is just a simple
extension of the idempotent producer except with a manual begin transaction and
end transaction call whenever appropriate.

EOS has been integration tested, but I would love more verification of it.