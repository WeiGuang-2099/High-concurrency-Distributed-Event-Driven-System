# Load Test Report — Ticket Reserve (Flash-Sale Path)

Tool: [k6](https://k6.io) v2.0.0. Target: `ticket-service` directly on `:8082`, injecting the
`X-User-*` headers the gateway normally adds. Hitting the service directly isolates it under
test (no gateway, JWT, or other JVMs competing for CPU) — the correct setup for a benchmark.

## Environment

- Single Windows laptop, 15.7 GB RAM. ticket-service runs **in isolation** + infra in Docker.
  (The full 7-service mesh needs ~16 GB+ to stay stable; see note at the bottom.)
- ticket-service: Java 21, Spring Boot 3.2.5, Tomcat, HikariCP (default pool).
- Infra (Docker): MySQL 8, Redis 7, Kafka (KRaft), RabbitMQ.
- Reserve path under test: **Redis Lua** atomic check+decrement → **MySQL** (insert reservation +
  `UPDATE ... reserved_quantity+1` on the stock row) → afterCommit: Kafka event + RabbitMQ delayed message.

### Launching ticket-service for the benchmark

Start the service in isolation with these JVM/Spring overrides (required on Windows — see
"Required flags" below):

```bash
java -jar ticket-service/target/ticket-service-1.0.0-SNAPSHOT.jar \
  --seata.enabled=false \
  --server.tomcat.accept-count=1000 \
  --server.tomcat.max-connections=4096 \
  --server.tomcat.threads.max=300
```

**Required flags (why):**

- `--server.tomcat.accept-count=1000` — **the one that actually matters.** Tomcat's default
  accept-count (the OS listen backlog) is **100**, which is smaller than the 200-VU connection
  burst. k6 opens all VU connections near-simultaneously; the backlog overflows and Windows
  RSTs the excess, which k6 reports as `connectex: ... actively refused`. With `shared-iterations`,
  each refused connection fails its iteration and the VU immediately retries, so a small initial
  overflow cascades into a near-total failure. Single-/low-VU runs are unaffected. Raising the
  backlog past the VU count eliminates the refusals entirely.
- `--seata.enabled=false` — the Seata server (in Docker) registers its **container-internal IP**
  (e.g. `172.26.0.8:8091`) to Nacos, which the host JVM cannot reach, producing a per-second
  reconnect error storm. The reserve path uses a plain local `@Transactional` (not
  `@GlobalTransactional`), so Seata is pure noise here. Disabling it removes the log spam and
  ~22 s of startup time; it was **not** the cause of the connection refusals.

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
| reserve_success | **10000 / 10000** |
| reserve_errors (5xx) | **0** |
| http_req_failed | **0.00%** (0 / 10002) |
| throughput | ~261 req/s (sustained, all-success path) |
| latency | p95 = 903 ms, p99 = 1.17 s |
| final stock | reserved=10000, available=0 |
| **oversell check** | **PASS** — reserved+sold (10000) ≤ total (10000) |

**All 10000 reserves succeeded with zero socket-level failures.** (An earlier run on the default Tomcat
backlog lost 563 requests to connection refusals — that was the `accept-count` issue documented above,
not a server fault; raising the backlog eliminated it.) p95 = 903 ms exceeds the 800 ms threshold here:
that threshold is calibrated for the oversell scenario, where ~95% of requests are fast out-of-stock
rejections; on this all-success path every request runs the full MySQL insert + hot-row `UPDATE` +
synchronous RabbitMQ send, so higher latency is expected. Correctness (zero oversell, zero errors,
10000/10000 reserved) is unaffected.

## Key findings

- **Headline: zero oversell**, verified by a post-run invariant (`reserved + sold ≤ total`) under 200
  concurrent VUs, in both small- and large-stock runs.
- **Redis-Lua ↔ MySQL stay consistent** — Redis net decrements always equal MySQL `reserved_quantity`,
  even at saturation.
- The throughput ceiling on this hardware is gated by the **MySQL success path** (row insert + hot-row
  `UPDATE` + synchronous RabbitMQ send in `afterCommit`), not by the Redis check. This is the natural
  next optimization target.
- **Optimistic (Redis-Lua) beats pessimistic (`SELECT ... FOR UPDATE`) by ~1.5x throughput** at equal
  correctness (Scenario 3): moving the contention check into Redis shortens how long each request holds
  the MySQL row lock.

## Scenario 3 — Optimistic (Redis-Lua) vs Pessimistic-lock head-to-head

`STOCK=8000, ITERATIONS=8000, VUS=50` for both, all-success path. `POST /api/tickets/reserve` (Redis-Lua
atomic check + local `@Transactional`) vs `POST /api/tickets/reserve-pessimistic` (`SELECT ... FOR UPDATE`
on the stock row, no Redis).

| metric | Redis-Lua (optimistic) | `SELECT ... FOR UPDATE` (pessimistic) |
|---|---|---|
| reserve_success | 8000 / 8000 | 8000 / 8000 |
| reserve_errors (5xx) | 0 | 0 |
| **throughput** | **~282 req/s** | **~185 req/s** |
| latency p95 | 218 ms | 332 ms |
| latency p99 | 243 ms | 385 ms |
| latency avg / max | 176 ms / 268 ms | 269 ms / 598 ms |
| **oversell check** | **PASS** | **PASS** |

**Both prevent oversell; Redis-Lua delivers ~1.5x the throughput at lower latency.** The pessimistic
variant serializes every reserve on the row lock for the *whole* transaction (lock held across the insert
+ update + commit), so concurrent buyers queue behind the lock. Redis-Lua does the contention check
atomically in memory and only touches MySQL for the (still row-contended but shorter) write, so the lock
is held for less of each request — hence higher throughput and a tighter latency tail.

## Reproduce

First start infra (Docker) and launch ticket-service **with the required flags** from
"Launching ticket-service for the benchmark" above. Then (k6 v2.0.0; `winget install GrafanaLabs.k6`):

```bash
# oversell proof (Lua)
k6 run -e STOCK=100   -e ITERATIONS=2000  -e VUS=200 loadtest/ticket-rush.js
# successful-reserve throughput (Lua)
k6 run -e STOCK=10000 -e ITERATIONS=10000 -e VUS=200 loadtest/ticket-rush.js

# head-to-head comparison: optimistic Redis-Lua vs pessimistic SELECT ... FOR UPDATE
k6 run -e ENDPOINT=/api/tickets/reserve              -e STOCK=8000 -e ITERATIONS=8000 -e VUS=50 loadtest/ticket-rush.js
k6 run -e ENDPOINT=/api/tickets/reserve-pessimistic  -e STOCK=8000 -e ITERATIONS=8000 -e VUS=50 loadtest/ticket-rush.js
```

> All four runs above were re-verified on 2026-06-26 (k6 v2.0.0) — zero oversell, zero `reserve_errors`
> on every run. Note k6's `http_req_failed` is high on the oversell run only because it counts the
> out-of-stock **400**s (an expected business rejection) as non-2xx; the script's own `reserve_errors`
> counter (5xx / unexpected only) is the meaningful error metric.

> Note: this run measured the service in isolation. Running all 7 services + full infra on a 15.7 GB
> machine exhausts RAM (free ~1.4 GB), which destabilizes the gateway's Nacos/Redis connections — so
> gateway-routed end-to-end load testing needs a bigger host or fewer co-located services.
