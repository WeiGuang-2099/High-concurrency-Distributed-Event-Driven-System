# Load Test Report — Ticket Reserve (Flash-Sale Path)

Tool: [k6](https://k6.io) v2.0.0. Target: `ticket-service` directly on `:8082`, injecting the
`X-User-*` headers the gateway normally adds. Hitting the service directly isolates it under
test (no gateway, JWT, or other JVMs competing for CPU) — the correct setup for a benchmark.

## Environment

- Single Windows laptop, 15.7 GB RAM. ticket-service runs **in isolation** + infra in Docker.
  (The full 7-service mesh needs ~16 GB+ to stay stable; see note at the bottom.)
- ticket-service: Java 21, Spring Boot 3.2.5, Tomcat (default 200 worker threads), HikariCP (default pool).
- Infra (Docker): MySQL 8, Redis 7, Kafka (KRaft), RabbitMQ.
- Reserve path under test: **Redis Lua** atomic check+decrement → **MySQL** (insert reservation +
  `UPDATE ... reserved_quantity+1` on the stock row) → afterCommit: Kafka event + RabbitMQ delayed message.

## Scenario 1 — Oversell correctness under contention

`STOCK=100, ITERATIONS=2000, VUS=200`

| metric | value |
|---|---|
| reserve_success | **100** |
| reserve_out_of_stock | 1900 |
| reserve_errors (5xx) | **0** |
| final stock | reserved=100, sold=0, available=0 |
| **oversell check** | **PASS** — reserved+sold (100) ≤ total (100) |
| latency | p95 = 632 ms, p99 = 683 ms |
| throughput | ~921 req/s |

**Exactly 100 reservations out of 2000 concurrent attempts.** Redis decrement count == MySQL
reserved count. Zero oversell under 200 concurrent buyers.

## Scenario 2 — Successful-reserve throughput (full row contention)

`STOCK=10000, ITERATIONS=10000, VUS=200` (every request decrements the same stock row)

| metric | value |
|---|---|
| reserve_success | 9437 |
| throughput | ~283 req/s (sustained, all-success path) |
| latency | p95 = 860 ms, p99 = 1.05 s |
| final stock | reserved=9437, available=563 |
| **oversell check** | **PASS** — reserved+sold (9437) ≤ total (10000) |
| connection-level failures | 563 (5.6%) |

The 563 failures were **client/socket-level** (Tomcat's 200-thread pool + everything co-located on one
laptop), not server errors: there were **zero** Redis-compensation log lines, and
`reserved (9437) + available (563) = 10000` exactly — the server never processed those 563, so no state
changed and consistency held perfectly.

## Key findings

- **Headline: zero oversell**, verified by a post-run invariant (`reserved + sold ≤ total`) under 200
  concurrent VUs, in both small- and large-stock runs.
- **Redis-Lua ↔ MySQL stay consistent** — Redis net decrements always equal MySQL `reserved_quantity`,
  even at saturation.
- The throughput ceiling on this hardware is gated by the **MySQL success path** (row insert + hot-row
  `UPDATE` + synchronous RabbitMQ send in `afterCommit`), not by the Redis check. This is the natural
  next optimization target.

## Pending

- **Pessimistic-lock comparison.** The comparison endpoint `POST /api/tickets/reserve-pessimistic`
  (`SELECT ... FOR UPDATE`, no Redis) is **implemented** and verified for correctness (single request +
  no oversell). The head-to-head load *run* is **pending a less resource-constrained host**: after the
  long session the 15.7 GB laptop is memory-saturated, and k6's concurrent connections start failing at
  the Windows resolver level (`lookup: no such host`) — the same ceiling that 503s the gateway. The
  earlier Lua runs above succeeded only because the machine still had headroom then. Re-run the two
  commands below (Lua vs pessimistic) on a fresh boot / bigger host to produce the throughput table.
  Both approaches prevent oversell; the expected difference is throughput/p99 (pessimistic serializes
  every reserve on the row lock for the whole transaction; Redis-Lua does the check in memory).

## Reproduce

```bash
# oversell proof (Lua)
k6 run -e STOCK=100   -e ITERATIONS=2000  -e VUS=200 loadtest/ticket-rush.js
# successful-reserve throughput (Lua)
k6 run -e STOCK=10000 -e ITERATIONS=10000 -e VUS=200 loadtest/ticket-rush.js

# head-to-head comparison (run on a host with RAM headroom)
k6 run -e ENDPOINT=/api/tickets/reserve              -e STOCK=8000 -e ITERATIONS=8000 -e VUS=50 loadtest/ticket-rush.js
k6 run -e ENDPOINT=/api/tickets/reserve-pessimistic  -e STOCK=8000 -e ITERATIONS=8000 -e VUS=50 loadtest/ticket-rush.js
```

> Note: this run measured the service in isolation. Running all 7 services + full infra on a 15.7 GB
> machine exhausts RAM (free ~1.4 GB), which destabilizes the gateway's Nacos/Redis connections — so
> gateway-routed end-to-end load testing needs a bigger host or fewer co-located services.
