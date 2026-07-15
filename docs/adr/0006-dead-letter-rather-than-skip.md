# 6. A consumer that cannot process an event must dead-letter it, never skip it

Date: 2026-07-15
Status: Accepted

## Context

Every Kafka consumer here runs with manual acknowledgement. That gives each listener a choice when
processing fails, and the same wrong choice was made independently in six places:

```java
} catch (Exception e) {
    log.error("...", e);
} finally {
    ack.acknowledge();   // acked regardless
}
```

This looks defensive and is the opposite. Acking a failed record commits its offset: the event is
gone, with a log line as the only trace. Worse, it defeats the recovery machinery that was already
configured — `DeadLetterPublishingRecoverer` is wired onto the shared listener factory, but a
`DefaultErrorHandler` only ever sees exceptions that **escape** the listener. Catching the
exception means the recoverer never runs. The DLT existed and was unreachable.

The variants found:

- Four event-store projections (`bid_history`, `auction_summary`, `order_timeline`,
  `stock_movements`) — catch-log-ack. Fixed in `382dba2`.
- `notification-service` — no recoverer on its error handler at all, so spring-kafka retried three
  times and then skipped. Not merely a lost WebSocket push: the OUTBID and AUCTION_SETTLED branches
  persist a user-facing `Notification` row. Fixed in `ee51241`.
- `EventStoreConsumer` — the same shape, fixed earlier in `bed8653`. Its four neighbours kept the
  original bug for three more months.

## Decision

A listener that cannot process an event **lets the exception escape**. It may log first for
context, but it must rethrow and must not acknowledge. The container's `DefaultErrorHandler` then
retries with backoff and publishes to the source topic's DLT.

Corollaries:

- Every error handler gets a recoverer. "Retry then skip" is silent loss wearing a retry policy.
- Each DLT is declared with the **same partition count** as its source topic, because the recoverer
  republishes to the partition the record came from and fails if the broker auto-created the DLT
  with fewer.
- The source-topic-to-DLT mapping lives in `KafkaTopics.dltFor` — every service that dead-letters
  has to agree on the names.
- Deliberate exceptions are allowed but must be *visible*: `EventStoreConsumer` swallows cache
  invalidation failures on purpose (a stale cache is an optimisation problem, not a consistency
  one) and says so in a comment. The rule is not "never catch", it is "never catch silently".

## Consequences

- Failures become loud and recoverable instead of quiet and permanent. A poisoned event parks in
  the DLT where it can be inspected and replayed.
- A genuinely un-processable event now blocks its partition through the retry cycle before
  dead-lettering, where before it vanished instantly. That is the correct trade: throughput is
  recoverable, data is not.
- Enforced by `ProjectionErrorHandlingTest` — for each projection, a repository failure must
  propagate and must not ack. The four failure cases fail against the previous code, so the test
  would catch a regression to the old shape.
- **The lesson worth keeping:** this bug was fixed once, in `bed8653`, and the fix was not applied
  to the four listeners sitting in the same package doing the same thing. Fixing an instance is not
  fixing the class of bug; the next step after any real defect is to grep for its shape.
