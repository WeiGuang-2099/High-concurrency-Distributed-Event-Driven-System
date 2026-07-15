# 2. Guard limited stock with a Redis Lua script, not a database lock

Date: 2026-06-25 (recorded retrospectively 2026-07-15)
Status: Accepted

## Context

Two paths must never oversell: reserving a ticket from limited stock, and accepting a bid only
if it beats the current highest. Both are read-then-write on a single hot row that thousands of
users hit at once — the classic lost-update race.

The conventional answer is to make the database serialize it: `SELECT ... FOR UPDATE`, or an
optimistic `UPDATE ... WHERE version = ?` with a retry loop. That is correct and simple, and it
puts every concurrent buyer in a queue behind one row lock for the duration of a transaction.

## Decision

Do the compare-and-set in Redis, in a Lua script (`reserve_ticket.lua`, `place_bid.lua`).

Redis runs a script atomically against its single-threaded core, so the check and the decrement
cannot interleave — the same guarantee a row lock gives, without a transaction spanning a network
round trip. MySQL remains the system of record; the Lua script is the admission controller in
front of it.

## Consequences

**Measured** (`loadtest/REPORT.md`, k6, same hardware):

- Zero oversell under 200 concurrent VUs: 2,000 simultaneous reserve attempts on 100 stock sold
  exactly 100. Redis and MySQL agree afterwards.
- ~1.5x the throughput of the pessimistic-lock implementation at lower tail latency (Scenario 3,
  head-to-head against `SELECT ... FOR UPDATE` on the same data).
- The bid path, which answers from Redis alone, sustains multiples of the reserve success path —
  the reserve path still does its MySQL write on the request thread, which is what bounds it.

**The cost, stated plainly:** correctness now depends on Redis and MySQL agreeing, and they are
two systems. That obligation is real and shows up in three places in this repo:

- `AuctionStateWarmer` rebuilds Redis bidding state from MySQL on startup, because Redis is not
  the durable record and a restart with an empty Redis would silently accept bids below the true
  highest.
- The bid path needed ADR-0005 to make an accepted bid durable at all.
- Every load test asserts convergence (Redis == MySQL == max persisted row) rather than assuming
  it, because "they agree" is the assumption most likely to be quietly false.

A row lock would have needed none of that. This is a throughput-for-complexity trade, and it is
only worth it because these two paths are the contended ones. Everything else in the system uses
ordinary transactions.
