# 5. Make accepted bids durable with a Redis Stream outbox

Date: 2026-07-15
Status: Accepted

## Context

ADR-0002 put bid admission in Redis: a Lua script decides whether a bid beats the current highest,
and MySQL persistence happens off the request path via a Kafka consumer. That leaves a gap the
original code did not close.

`placeBid` returned 201 the moment the script accepted, then published to Kafka fire-and-forget.
If the send failed, the bid existed only in the Redis highest hash — no Kafka event, no MySQL row.
Worse, it did not stay there: `AuctionStateWarmer` rebuilds Redis bidding state from the `bid`
table on startup, so the next restart would erase the bid and let a lower one become highest. The
user had a 201 in hand for a bid the system had forgotten.

The Scenario 5 load test reported "0 lost bids", which was true and misleading: it observed no
loss on a run where Kafka never faltered. Nothing prevented loss.

The textbook fix is a transactional outbox — insert the event into an outbox table in the same
transaction as the business write, and relay it. **It does not apply here.** There is no
transaction: the accept decision is made in Redis and the hot path never touches MySQL. Routing
bids through MySQL to get an outbox table would give back exactly the throughput ADR-0002 bought.

## Decision

Use Redis as the outbox, since Redis is where the decision is made.

`place_bid.lua` now `XADD`s the accepted bid to `auction:bid:outbox` **inside the same script**
that accepts it. Lua scripts are atomic, so accept-and-record is one step: there is no window
where a bid is accepted but unrecorded.

`BidOutboxRelay` drains the stream via `XREADGROUP` and acknowledges only after the broker
confirms the send. `BidOutboxReclaimer` `XCLAIM`s entries left pending by a relay that died
between publishing and acknowledging — without it the outbox would still lose those bids, quietly,
which is the failure mode the pattern exists to prevent.

Delivery is therefore at-least-once, so **event ids are derived from the stream entry id**
(`<entry-id>:placed`, `<entry-id>:outbid`) rather than freshly generated. A replay must carry the
id consumers already dedupe on: the `bid.event_id` unique key, and the read model as of `6e58e88`.
A random UUID per attempt would have turned every retry into a duplicate bid.

## Consequences

- **Durability is now Redis's**, and it is only as good as Redis persistence. With AOF on and
  `appendfsync everysec`, a hard crash can lose up to a second of accepted bids. This is the trade
  for keeping bid accept off MySQL; it is recorded in `BidOutboxRelay`'s javadoc rather than left
  for someone to discover. `appendfsync always` would close it at a cost the hot path would feel.
  *Correction, 2026-11-10:* this ADR originally called `everysec` "the default", which is only
  true once AOF is enabled. The `redis:7-alpine` image ships with AOF off, and `docker-compose.yml`
  did not turn it on, so as first shipped the exposure was the gap between RDB snapshots (minutes),
  not one second. Compose now starts Redis with `--appendonly yes --appendfsync everysec` and a
  data volume, which is what makes the bound above hold.
- **Throughput cost is below measurement noise** (`REPORT.md` Scenario 6): outbox 2,425-2,675
  bids/s against a same-session baseline of 2,512-3,004. Only ~4% of requests — the accepted ones
  — reach the `XADD` at all.
- `MAXLEN ~` bounds the stream so a stuck relay cannot exhaust memory. Approximate trimming only
  discards entries once the relay is roughly `maxLen` behind, at which point relay lag is the real
  alarm and events are already being lost.
- The relay needs **its own Redis connection**: `XREADGROUP` blocks for a second, while the shared
  Lettuce factory runs a 200 ms command timeout on purpose so a slow Redis fails the bid hot path
  fast. Those two requirements cannot share a timeout.
- Single-node Redis only. The three keys share no hash tag, so the script would be rejected in
  cluster mode — as it already would have been with two keys. Cluster support would need the
  outbox partitioned per auction, or a hash tag scheme.
- This closes the bid path only. The MySQL-backed paths keep their dual-write gap until they get
  the outbox the textbook actually describes — ticket-service now has one, see
  [ADR-0007](0007-transactional-outbox-for-mysql-backed-paths.md). Auction lifecycle events
  (created / activated / expired / settled) are still published directly from `AuctionServiceImpl`
  and `AuctionLifecycleScheduler` after a MySQL write, as are order-service's events; both remain
  open rather than being claimed as solved here.
