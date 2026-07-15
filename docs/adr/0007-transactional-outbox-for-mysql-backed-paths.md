# 7. Use a transactional outbox table on the MySQL-backed paths

Date: 2026-07-15
Status: Accepted (implemented in ticket-service; order and auction still open)

## Context

[ADR-0005](0005-redis-stream-outbox-for-bid-durability.md) gave the bid path a Redis Stream
outbox and said plainly that it closed the bid path only. This is the other half.

The ticket path writes MySQL and then publishes. It was already doing the better version of
the naive thing — publishing from an `afterCommit` callback rather than inside the transaction,
so a rolled-back reserve never emitted a `StockReserved`. What it could not survive is the
process dying between the commit and the send: stock has moved, the reservation exists, and no
event ever existed. The read models, the event store, and the notifications all just never hear
about it, silently.

Unlike the bid path, this one *has* a transaction to hang an outbox off. So it gets the textbook
answer rather than a variant.

## Decision

Insert the event into an `outbox` table in the same transaction as the business change, and let
a relay publish it afterwards.

- `TicketEventProducer` keeps its public API but now writes rows instead of sending to Kafka. It
  is annotated `@Transactional(propagation = MANDATORY)`: the outbox is only meaningful inside
  the business transaction, so being called without one is a bug that should fail loudly rather
  than quietly degrade into the second write it was meant to eliminate.
- Every call site moved out of `afterCommit` and into the transaction body. What stayed in
  `afterCommit` is what is genuinely not transactional: the Redis stock refund (refunding before
  a rollback would hand back stock that was never released) and the RabbitMQ delayed-release
  timer (no point scheduling a release for a reservation that may roll back).
- `OutboxRelay` claims a batch, publishes, and stamps `sent_at` only once the broker acknowledges.
  Claim-publish-mark is deliberately three steps: holding the row lock across the Kafka round trip
  would put a network call inside a transaction, which is the mistake `ebf22d1` already fixed once
  on the payment path.
- A claim older than the stale window is retaken, so a relay that dies mid-publish does not strand
  its batch. Same role as `XCLAIM` on the bid outbox.

## Consequences

- **Delivery is at-least-once**, like every other publisher here: a crash between the send and the
  `sent_at` stamp republishes the row. Harmless, because the event id travels with the event and
  consumers dedupe on it (`bid.event_id`'s unique key, the read model as of `6e58e88`).
- **The relay must rebuild the concrete event**, not ship the stored JSON. The payload carries no
  type information, so the row also stores `payload_class`. Publishing the raw string would set the
  consumers' `__TypeId__` header to `java.lang.String` and every listener would skip the event —
  the exact failure mode of `5faea68`. Deserializing to `BaseEvent` would be worse: it compiles,
  publishes, and silently drops every subtype field. `Class.forName` on a stored value is limited
  to the event package.
- **Events are now delayed by up to one poll interval** (1 s) rather than being sent at commit
  time. Nothing on this path is latency-sensitive — the request already returned.
- A row that can never be published (a genuine poison event) is retried indefinitely with an error
  log rather than dead-lettered. Retrying forever is the safer default of the two, but it is a gap:
  the Kafka consumers get a DLT and the outbox does not.
- **Not done yet:** order-service and auction-service still publish after their MySQL writes and
  keep the gap. The mechanism is proven here and is meant to be copied; claiming otherwise would
  make this ADR the same kind of half-truth ADR-0005 was careful to avoid.

## Why two outbox implementations

Because there are two sources of truth. The bid path decides in Redis and never opens a MySQL
transaction — routing bids through MySQL purely to obtain an outbox table would give back exactly
the throughput [ADR-0002](0002-redis-lua-for-oversell-protection.md) bought. The ticket path
already has a transaction, so inventing a Redis-based outbox for it would add a second store to a
problem that did not need one. Same guarantee, same claim-publish-ack shape, different substrate
because the decision is made in a different place.
