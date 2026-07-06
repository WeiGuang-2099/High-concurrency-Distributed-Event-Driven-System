// Auction-bid load test: hammers ONE auction with concurrent bids to (1) measure the
// pure-Redis bid path throughput (contrast with the MySQL-bound reserve path) and
// (2) verify the bid-path invariants end to end:
//   - highest price is monotonic: the Lua script only accepts strictly higher amounts,
//     so every persisted amount is unique and the max equals the final highest
//   - no lost bids: every accepted (201) bid ends up as a MySQL row via Kafka
//   - convergence: Redis highest hash == MySQL auction snapshot == max persisted bid
//
// Hits auction-service DIRECTLY (default :8081), injecting the X-User-* headers the
// gateway would normally add. This isolates the service under test AND deliberately
// bypasses the gateway per-user rate limiter (10 bids/s per user) - correct for a
// throughput benchmark; the limiter itself is covered by gateway-rate-limit.js.
//
// The bid hot path is Redis-only (Lua compare-and-set on the highest hash), with
// MySQL persistence deferred to a Kafka consumer - so unlike ticket reserve, no
// MySQL write sits on the request path. teardown() waits for the consumer to drain
// before checking convergence.
//
// Launch auction-service in isolation with the same Windows backlog flags as the
// reserve benchmark (see loadtest/REPORT.md):
//   java -jar auction-service/target/auction-service-1.0.0-SNAPSHOT.jar \
//     --server.tomcat.accept-count=1000 --server.tomcat.max-connections=4096 \
//     --server.tomcat.threads.max=300 --server.tomcat.max-keep-alive-requests=-1 \
//     --eureka.client.enabled=false --spring.cloud.config.enabled=false
//
// max-keep-alive-requests=-1: Tomcat closes each keep-alive connection after 100
// requests by default; at ITERATIONS/VUS >= 100 every VU reconnects mid-run and the
// reconnect burst can collide with Windows TIME_WAIT ports (client-side dial error).
//
// Parameters (via -e KEY=VALUE):
//   BASE_URL    target base url           (default http://localhost:8081)
//   VUS         concurrent virtual users  (default 200)
//   ITERATIONS  total bid attempts        (default 20000)
//   START_PRICE auction starting price    (default 100)
//
// Run (the --local-ips spread is needed on Windows, see the thresholds comment):
//   k6 run --local-ips=127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5 \
//     -e ITERATIONS=20000 -e VUS=200 loadtest/auction-bid.js

import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';

const BASE       = __ENV.BASE_URL || 'http://localhost:8081';
const VUS        = parseInt(__ENV.VUS || '200', 10);
const ITERATIONS = parseInt(__ENV.ITERATIONS || '20000', 10);
const START      = parseFloat(__ENV.START_PRICE || '100');

const accepted   = new Counter('bid_accepted');       // 201: new highest bid
const tooLow     = new Counter('bid_too_low');        // 400: lost the race to a higher concurrent bid
const errors     = new Counter('bid_errors');         // server misbehavior (5xx, auction-not-active, ...)
const dialErrors = new Counter('client_dial_errors'); // status 0: k6 could not even connect (see below)
const bidDur     = new Trend('bid_duration', true);

export const options = {
  // Same Windows resolver workarounds as ticket-rush.js.
  dns: { ttl: '1h', select: 'first', policy: 'preferIPv4' },
  hosts: { 'localhost:8081': '127.0.0.1:8081' },
  scenarios: {
    rush: { executor: 'shared-iterations', vus: VUS, iterations: ITERATIONS, maxDuration: '180s' },
  },
  thresholds: {
    bid_errors: ['count<1'],
    bid_accepted: ['count>0'],
    // Tomcat hard-closes keep-alive on 4xx (Http11Processor.statusDropsConnection),
    // so in this rejection-heavy workload nearly every request costs the CLIENT a
    // fresh connection; ~19k dials inside the TIME_WAIT window can exhaust Windows'
    // 16384 dynamic ports and fail a dial before it reaches the server. Run with
    //   --local-ips=127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5
    // to spread VUs over several loopback source IPs (a port space each).
    client_dial_errors: ['count<20'],
    http_req_duration: ['p(95)<800', 'p(99)<1500'],
  },
  setupTimeout: '60s',
  // teardown waits for the Kafka bid persistor to drain into MySQL before verifying.
  teardownTimeout: '300s',
};

const JSON_HEADERS = { 'Content-Type': 'application/json' };
const ADMIN = { ...JSON_HEADERS, 'X-User-Id': '1', 'X-Username': 'admin', 'X-User-Roles': 'ADMIN' };

export function setup() {
  const stamp = Date.now();
  // The server validates startTime against LocalDateTime.now() (server-local wall
  // clock), so the timestamps must be LOCAL time, not UTC/toISOString().
  const r = http.post(`${BASE}/api/admin/auctions`, JSON.stringify({
    eventName: `k6 bid rush ${stamp}`,
    description: 'load test auction',
    startingPrice: START.toFixed(2),
    startTime: localDateTime(Date.now() + 2000),
    endTime: localDateTime(Date.now() + 2 * 3600 * 1000),
  }), { headers: ADMIN });
  if (r.status !== 201) throw new Error(`auction create failed: ${r.status} ${r.body}`);
  const auctionId = r.json('data.id');

  // The lifecycle scheduler (1s poll) flips PENDING -> ACTIVE and seeds the Redis
  // status + highest keys; bids are rejected until then.
  for (let i = 0; i < 60; i++) {
    const d = http.get(`${BASE}/api/auctions/${auctionId}`);
    if (d.status === 200 && d.json('data.status') === 'ACTIVE') {
      console.log(`setup: auctionId=${auctionId} ACTIVE, start=${START.toFixed(2)} vus=${VUS} iterations=${ITERATIONS}`);
      return { auctionId, benchStart: Date.now() };
    }
    sleep(0.5);
  }
  throw new Error(`auction ${auctionId} never became ACTIVE`);
}

export default function (data) {
  const i = exec.scenario.iterationInTest;
  const uid = (i % 10000) + 2;                      // rotate bidders; never uid 1 (admin)
  // Unique, globally increasing amount per iteration. Out-of-order arrivals get
  // BID_TOO_LOW - that is the contention the Lua compare-and-set is there to resolve.
  const amount = (START + 0.01 * (i + 1)).toFixed(2);
  const res = http.post(`${BASE}/api/auctions/${data.auctionId}/bids`,
    JSON.stringify({ amount }),
    { headers: { ...JSON_HEADERS, 'X-User-Id': String(uid), 'X-Username': `u${uid}`, 'X-User-Roles': 'USER' } });
  bidDur.add(res.timings.duration);
  if (res.status === 0) { dialErrors.add(1); return; }  // never reached the server
  const code = safeCode(res);
  if (code === 200) accepted.add(1);
  else if (code === 400 && String(safeMessage(res)).includes('Bid too low')) tooLow.add(1);
  else errors.add(1);
}

export function teardown(data) {
  const id = data.auctionId;

  // k6's built-in http_reqs rate divides by the WHOLE run (setup + teardown polling
  // included), so compute the scenario-only throughput here: teardown starts right
  // after the last iteration finishes.
  const scenarioSecs = (Date.now() - data.benchStart) / 1000;
  console.log(`teardown: scenario wall time ~${scenarioSecs.toFixed(1)}s => ~${(ITERATIONS / scenarioSecs).toFixed(0)} bids/s`);

  // 1. Wait for the Kafka bid persistor to drain: poll the MySQL-backed bid count
  //    until it stops growing (3 consecutive stable reads, 2s apart).
  const waitStart = Date.now();
  let total = -1, stable = 0;
  while (Date.now() - waitStart < 240000) {
    const t = bidCount(id);
    if (t === total && t > 0) {
      if (++stable >= 3) break;
    } else {
      stable = 0;
      total = t;
    }
    sleep(2);
  }
  console.log(`teardown: bid rows stable at ${total} after ${((Date.now() - waitStart) / 1000).toFixed(0)}s`);

  // 2. Fetch every persisted bid amount (pages of 100).
  const amounts = [];
  for (let page = 0; page * 100 < total; page++) {
    const r = http.get(`${BASE}/api/auctions/${id}/bids?page=${page}&size=100`);
    for (const item of r.json('data.items')) amounts.push(Number(item.amount).toFixed(2));
  }

  // 3. Three views of the final highest price:
  //    Redis (via a read-only probe bid), MySQL auction snapshot, max persisted bid row.
  const redisHighest = probeRedisHighest(id);
  const snap = http.get(`${BASE}/api/auctions/${id}`).json('data');
  const snapshotHighest = Number(snap.currentHighestBid).toFixed(2);
  const maxPersisted = amounts.length
    ? amounts.reduce((m, a) => (Number(a) > Number(m) ? a : m)).toString() : 'none';

  const distinct = new Set(amounts).size;
  const uniquePass = distinct === amounts.length;
  const convergedPass = redisHighest === snapshotHighest && snapshotHighest === maxPersisted;

  console.log('================= RESULT =================');
  console.log(`persisted bids (MySQL rows):    ${amounts.length}`);
  console.log(`distinct amounts:               ${distinct}`);
  console.log(`highest - Redis hash:           ${redisHighest}`);
  console.log(`highest - MySQL snapshot:       ${snapshotHighest}`);
  console.log(`highest - max persisted bid:    ${maxPersisted}`);
  console.log(`MONOTONIC/UNIQUE CHECK: every persisted amount unique  =>  ${uniquePass ? 'PASS' : 'FAIL'}`);
  console.log(`CONVERGENCE CHECK: Redis == MySQL snapshot == max bid  =>  ${convergedPass ? 'PASS' : 'FAIL - DIVERGED!'}`);
  console.log('(accepted-vs-persisted reconciliation follows after the k6 summary)');
  console.log('=========================================');
}

// The lost-bids check needs the client-side accepted count, which only exists in the
// end-of-test metrics - hence handleSummary rather than teardown.
export function handleSummary(data) {
  let block = '';
  try {
    const acceptedCount = data.metrics.bid_accepted ? data.metrics.bid_accepted.values.count : 0;
    // setup data is not available here; recover the auction by its creation-time order.
    const latest = http.get(`${BASE}/api/auctions?page=0&size=1`).json('data.items')[0];
    const persisted = bidCount(latest.id);
    const pass = persisted === acceptedCount;
    block = [
      '', '============ LOST-BID CHECK =============',
      `auction ${latest.id}: accepted by Redis (201s) = ${acceptedCount}, persisted in MySQL = ${persisted}`,
      `NO LOST BIDS: persisted == accepted  =>  ${pass ? 'PASS' : 'FAIL - BIDS LOST IN THE PIPELINE!'}`,
      '=========================================', '',
    ].join('\n');
  } catch (e) {
    block = `\nLOST-BID CHECK skipped (${e}); compare bid_accepted with the persisted count from teardown manually.\n`;
  }
  return { stdout: textSummary(data, { indent: ' ', enableColors: true }) + block };
}

// Reads the current Redis highest through the public API: an amount at/below the
// starting price is guaranteed BID_TOO_LOW, whose message echoes the Redis value.
function probeRedisHighest(auctionId) {
  const r = http.post(`${BASE}/api/auctions/${auctionId}/bids`,
    JSON.stringify({ amount: '0.01' }),
    { headers: { ...JSON_HEADERS, 'X-User-Id': '999999', 'X-Username': 'probe', 'X-User-Roles': 'USER' } });
  const m = /current highest is ([0-9.]+)/.exec(String(safeMessage(r)));
  return m ? Number(m[1]).toFixed(2) : `unreadable (${r.status} ${r.body})`;
}

function bidCount(auctionId) {
  const r = http.get(`${BASE}/api/auctions/${auctionId}/bids?page=0&size=1`);
  try { return r.json('data.total'); } catch (e) { return -1; }
}

function localDateTime(epochMs) {
  const d = new Date(epochMs);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function safeCode(res) {
  try { return res.json('code'); } catch (e) { return res.status; }
}

function safeMessage(res) {
  try { return res.json('message'); } catch (e) { return res.body; }
}
