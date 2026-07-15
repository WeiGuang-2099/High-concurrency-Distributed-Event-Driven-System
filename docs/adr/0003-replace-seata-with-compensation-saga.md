# 3. Replace Seata AT with an application-level compensation saga

Date: 2026-06-26 (recorded retrospectively 2026-07-15)
Status: Accepted — supersedes the original Seata AT design

## Context

Settling an auction spans two services: reserve the winner's stock (ticket-service), then create
the order (order-service). If the order fails after the stock is reserved, the reservation must
not be left dangling.

The project originally used Seata AT mode, which gives this the shape of a local transaction:
annotate the entry point, and the framework snapshots before/after images into `undo_log` and
rolls participants back automatically.

Two things forced a re-examination:

1. Seata AT never actually worked here. It contributed a `DataSourceProxy` circular-reference
   bean failure in three services that blocked startup, and it was one of the reasons the system
   had never run end-to-end.
2. The workload does not justify it. Settlement fires once per auction, from a scheduler — it is
   not on any hot path. 2PC's cost (locks held across services for the duration, plus a
   transaction coordinator that must be up for settlement to work at all) buys atomicity that
   this path can get more cheaply.

## Decision

Remove Seata entirely and write the saga by hand in `SettlementService.settle`: reserve stock via
Feign, then create the order; if order creation fails, call ticket-service's
`/internal/settle-release` to compensate, then rethrow so the scheduler reverts the auction to
ACTIVE and retries.

Make both participants idempotent, because a retried saga will re-run them:

- `createFromAuction` returns the existing AUCTION order for the same auction id.
- `settleRelease` is a no-op unless the reservation is still PENDING (`updateStatusIfPending`).

## Consequences

- No transaction coordinator to deploy, run, or explain. One fewer single point of failure for a
  path that must eventually succeed.
- The compensation is application code, so it is testable without infrastructure:
  `SettlementServiceTest` covers the success path, compensation on order failure (both returned
  error and thrown exception), no compensation when the reserve itself failed, and the original
  error surviving when compensation also fails. `SettlementSagaIT` runs it over real HTTP.
- **The trade:** there is a real window where stock is reserved and no order exists. The saga is
  eventually consistent, not atomic — a reader between the two steps sees an intermediate state.
  For settlement that is fine (nobody is watching a single auction settle in real time); for the
  ticket reserve path it would not be, which is why that one is a single local transaction.
- Compensation can itself fail. When it does, the code preserves the original error and lets the
  scheduler retry, rather than reporting a compensation failure that hides the real cause.
- Removing `seata-all` broke `auction-common`'s compilation, because it had been supplying
  `spring-web` and `spring-context` transitively. Only the first was replaced, and the missing
  `spring-context` went unnoticed for ten days — see `d3b1782`. A dependency removed from a pom
  is not the same as a dependency that was never needed.
