# Load Test Report

Tool: [k6](https://k6.io) v2.0.0. Scenarios 1-3 benchmark the ticket reserve (flash-sale) path
against `ticket-service` directly on `:8082`; Scenario 4 proves the gateway per-user rate
limiter (through the gateway, by design); Scenario 5 benchmarks the auction-bid path against
`auction-service` directly on `:8081`. Service benchmarks inject the `X-User-*` headers the
gateway normally adds — hitting the service directly isolates it under test (no gateway, JWT,
or other JVMs competing for CPU), the correct setup for a benchmark.

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
- **Removing MySQL from the request path entirely is worth ~13.6x** (Scenario 5): the auction-bid
  path answers from a Redis Lua compare-and-set and persists asynchronously via Kafka, sustaining
  ~3,560 bids/s against reserve's ~261 req/s — with a post-run invariant proving zero accepted
  bids were lost on the way to MySQL.

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

## Scenario 4 — Gateway per-user rate limiting (Redis token bucket)

Run 2026-07-06. Unlike Scenarios 1-3 (service benchmarked in isolation), this one goes **through
the gateway on :8080** — that is where the limiter lives. Stack: eureka-server, config-server,
gateway-service, user-service, ticket-service + Docker infra. The hot write routes
(`POST /api/tickets/reserve`, `POST /api/auctions/{id}/bids`) carry Spring Cloud Gateway's
`RequestRateLimiter` with the Redis token-bucket implementation, keyed by the JWT-derived
`X-User-Id` header. Reserve budget: **replenish 5 tokens/s, burst 10, per user**.

Two concurrent scenarios, all with real JWTs (users registered + logged in through the gateway
during setup):

| scenario | load | passed limiter | throttled (429) |
|---|---|---|---|
| hammer — 1 user | 30 req/s x 20 s = 601 requests | **106** | **495** |
| compliant — 5 users | 2 req/s each x 20 s = 200 requests | 200 | **0** |

- **The bucket math holds.** One user firing 30 req/s got 106 requests through — the theoretical
  budget is burst 10 + replenish 5/s x 20 s = **110**. The other 495 were rejected at the gateway
  with `429` + `X-RateLimit-*` headers and **never reached ticket-service**.
- **Per-user isolation.** Five compliant users sharing the same 20-second window saw **zero** 429s:
  one scripted abuser cannot starve legitimate buyers, because every user has an independent bucket
  (keyed by the `X-User-Id` the gateway derives from the verified JWT — clients cannot spoof it).
- Zero unexpected statuses (`rl_errors = 0`).

The same behavior is pinned down in `GatewayRateLimitIT` (gateway module, runs in `mvn verify`
against a real Redis container): burst-then-429 per user, throttled requests never reaching the
backend, per-user isolation, an independent bucket on the bid route, and non-hot routes bypassing
the limiter entirely.

## Scenario 5 — Auction bid throughput (pure-Redis hot path)

Run 2026-07-06. `VUS=200, ITERATIONS=20000`, every bid against **one** auction, hitting
auction-service directly on `:8081` (gateway deliberately bypassed — the per-user bid limiter
is proven separately in Scenario 4). The bid hot path never touches MySQL: a Lua script on the
`auction:{id}:highest` hash atomically rejects any bid not strictly above the current highest,
and accepted bids are persisted **off the request path** (Kafka `BidPlacedEvent` → bid-persistor
consumer → MySQL `bid` row + compare-and-set snapshot update, deduplicated by event id).

Each k6 iteration bids a unique, globally increasing amount (start price + 0.01 x iteration
number), so out-of-order arrivals lose the compare-and-set race exactly like real concurrent
bidders sniping one item.

| metric | value |
|---|---|
| **throughput** | **~3,560 bids/s** sustained (20,000 bids in 5.6 s) |
| latency | med = 57 ms, p95 = 71 ms, max = 87 ms (at 200-VU saturation) |
| bid_accepted (201, new highest) | 1,064 |
| bid_too_low (400, lost the CAS race) | 18,936 |
| bid_errors (5xx / unexpected) | **0** |
| **lost bids** (Redis 201s vs MySQL rows) | **0** — 1,064 accepted == 1,064 persisted |
| final highest: Redis hash / MySQL snapshot / max bid row | **300.00 == 300.00 == 300.00** |
| Kafka consumer drain time after the burst | ~14 s |

- **~13.6x the reserve success path** (~3,560 vs ~261 req/s, Scenario 2, same hardware). The
  two paths bracket a design tradeoff: a reserve success runs its MySQL insert + hot-row
  `UPDATE` synchronously on the request thread (strong synchronous consistency); a bid answers
  from Redis alone and defers durability to Kafka. The post-run invariants are what make that
  deferral defensible.
- **Invariants verified after the run** (in `teardown()` + `handleSummary()`):
  - *Monotonic highest*: the Lua script accepts only strictly-higher amounts, so all 1,064
    persisted amounts are unique and the max equals the final highest — verified across every
    persisted row.
  - *No lost bids*: k6-counted 201 responses == MySQL `bid` rows (the async pipeline dropped
    nothing; producer runs with `acks=all` + idempotence). **This run observed no loss; it did
    not prove loss was impossible** — the code returned 201 and then published fire-and-forget,
    so a broker hiccup would have lost the bid silently. Scenario 6 closes that with an outbox.
  - *Convergence*: Redis highest hash == MySQL auction snapshot == max persisted bid row. The
    Redis value is read through the public API by a probe bid at 0.01, whose `BID_TOO_LOW`
    message echoes the current Redis highest.
- med latency 57 ms at 200 VUs is queueing, not work: 200 concurrent / 3,560 req/s = 56 ms
  (Little's law). Accepted bids complete in ~2 ms of server time.
- **Client-side finding** (same family as the `accept-count` note above): Tomcat hard-closes
  keep-alive connections on 4xx responses (`Http11Processor.statusDropsConnection`), so this
  rejection-heavy workload costs the load generator a fresh connection per 400 — ~19k dials in
  a few seconds exhausted Windows' 16,384 dynamic ports (diagnosed via 9k+ `TIME_WAIT` sockets
  and a curl probe showing `Connection: close` on the 400s). Fix: spread VUs across loopback
  source IPs with `--local-ips` (one port space each); the script fails a threshold if any dial
  error slips through regardless.

## Scenario 6 — Bid outbox: does making the bid durable cost throughput?

Run 2026-07-15, after the bid path moved to a Redis Stream outbox (`place_bid.lua` now appends
every accepted bid to `auction:bid:outbox` inside the same script that accepts it; `BidOutboxRelay`
drains the stream to Kafka and acknowledges only once the broker has the event). Scenario 5's
"no lost bids" held in a run where nothing went wrong — it was not a guarantee, because the old
code returned 201 and then published fire-and-forget.

**The measurement question:** Scenario 5 recorded ~3,560 bids/s. Today the outbox build measured
~2,400-2,700. That looked like a 30% regression, but attributing it to the change would have been
wrong — only ~4% of requests (the accepted ones) even reach the `XADD`. So the old code was
re-measured **in the same session, on the same machine state**, by stashing the outbox work:

| build | runs | bids/s |
|---|---|---|
| baseline (no outbox, `6e58e88`) | 2 | 2,512 / 3,004 |
| outbox | 3 | 2,425 / 2,657 / 2,675 |

The two ranges overlap: **the outbox's cost is below this machine's run-to-run noise** (the
baseline alone spans ±20%). The ~3,560 figure in Scenario 5 is not reproducible today at all,
including on the code that produced it — it reflects a cleaner machine (fewer containers, less
memory pressure), not a faster design. Compare rows within a session, never across.

Invariants on every outbox run: **0 lost bids** (790 accepted == 790 persisted), convergence
`300.00 == 300.00 == 300.00`, all persisted amounts unique. Relay drained fully every time
(`XINFO GROUPS`: `pending 0`, `lag 0`).

**Two bugs this benchmark caught that the integration tests did not:**

1. **The relay was dead for an entire run** — 887 bids accepted, **0 persisted**. The listener's
   blocking `XREADGROUP` polls for 1 s, but the shared Lettuce factory runs a deliberately tight
   `timeout: 200ms` (the bid hot path wants to fail fast on a slow Redis). Every poll timed out,
   and Spring's `StreamPollTask` cancels the subscription on *any* error by default — so one
   timeout 0.2 s after startup killed the relay permanently, leaving one log line. The ITs missed
   it because they drive the relay directly and never exercise the listener container. Fixed with
   a dedicated connection factory for the relay (command timeout > poll timeout) and
   `cancelOnError(t -> false)`.
2. **`cancelOnError(t -> false)` then proved itself under real failure.** Back-to-back runs
   exhausted Windows' dynamic ports again (12,774 `TIME_WAIT`), which also starved the relay's
   Redis connections: `Unable to connect to Redis`, logged repeatedly. The relay kept polling,
   recovered when ports freed, and drained to `lag 0` — where the old default would have died on
   the first error.

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

# gateway per-user rate limiting (Scenario 4) - needs eureka-server, config-server,
# gateway-service, user-service and ticket-service running (no special Tomcat flags):
k6 run loadtest/gateway-rate-limit.js

# auction bid throughput (Scenario 5) - start auction-service in isolation first:
#   java -jar auction-service/target/auction-service-1.0.0-SNAPSHOT.jar \
#     --server.tomcat.accept-count=1000 --server.tomcat.max-connections=4096 \
#     --server.tomcat.threads.max=300 --server.tomcat.max-keep-alive-requests=-1 \
#     --eureka.client.enabled=false --spring.cloud.config.enabled=false
# (--local-ips works around the Tomcat-closes-on-4xx port exhaustion, see Scenario 5)
k6 run --local-ips=127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5 -e ITERATIONS=20000 -e VUS=200 loadtest/auction-bid.js
```

> The Scenario 1-3 runs above were re-verified on 2026-06-26 (k6 v2.0.0) — zero oversell, zero
> `reserve_errors` on every run. Note k6's `http_req_failed` is high on the oversell and bid runs
> only because it counts expected business rejections (out-of-stock / bid-too-low **400**s) as
> non-2xx; each script's own error counter (5xx / unexpected only) is the meaningful error metric.

> Note: Scenarios 1-3 measured the service in isolation. Running all 7 services + full infra on a
> 15.7 GB machine exhausts RAM (free ~1.4 GB), which destabilizes the gateway's discovery/Redis
> connections — so gateway-routed *throughput* benchmarks need a bigger host or fewer co-located
> services. Scenario 4 is gateway-routed but runs at a deliberately modest 40 req/s with only five
> JVMs up, which this machine handles fine.
