// Ticket-rush load test: hammers the ticket reserve endpoint to (1) prove zero
// oversell under high concurrency and (2) measure reserve throughput / latency.
//
// Hits ticket-service DIRECTLY (default :8082), injecting the X-User-* headers the
// gateway would normally add. This isolates the service under test (no gateway /
// JWT / other JVMs competing for CPU), which is the correct setup for a benchmark.
//
// Parameters (via -e KEY=VALUE):
//   BASE_URL    target base url           (default http://localhost:8082)
//   ENDPOINT    reserve path              (default /api/tickets/reserve)
//   STOCK       total stock to create     (default 100)
//   VUS         concurrent virtual users  (default 200)
//   ITERATIONS  total reserve attempts    (default 2000)
//
// Examples:
//   oversell proof:  k6 run -e STOCK=100   -e ITERATIONS=2000  ticket-rush.js
//   throughput:      k6 run -e STOCK=20000 -e ITERATIONS=20000 ticket-rush.js
//   pessimistic:     k6 run -e ENDPOINT=/api/tickets/reserve-pessimistic ... ticket-rush.js

import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const BASE       = __ENV.BASE_URL || 'http://localhost:8082';
const ENDPOINT   = __ENV.ENDPOINT || '/api/tickets/reserve';
const STOCK      = parseInt(__ENV.STOCK || '100', 10);
const VUS        = parseInt(__ENV.VUS || '200', 10);
const ITERATIONS = parseInt(__ENV.ITERATIONS || '2000', 10);
const TYPE       = 'VIP';

const success    = new Counter('reserve_success');
const outOfStock = new Counter('reserve_out_of_stock');
const errors     = new Counter('reserve_errors');
const reserveDur = new Trend('reserve_duration', true);

export const options = {
  // Resolve the host once and cache forever. Windows' DNS resolver fails ("no such host")
  // under many concurrent per-request lookups; pinning the cache avoids that entirely.
  dns: { ttl: '1h', select: 'first', policy: 'preferIPv4' },
  // Static host override so k6 never invokes the system resolver (Windows' resolver
  // fails under concurrent lookups when the host is resource-starved). Run with default
  // BASE_URL=http://localhost:8082 so this mapping applies.
  hosts: { 'localhost:8082': '127.0.0.1:8082' },
  scenarios: {
    rush: { executor: 'shared-iterations', vus: VUS, iterations: ITERATIONS, maxDuration: '180s' },
  },
  thresholds: {
    reserve_errors: ['count<1'],                 // any 5xx / unexpected response fails the run
    http_req_duration: ['p(95)<800', 'p(99)<1500'],
  },
};

const ADMIN = { 'Content-Type': 'application/json', 'X-User-Id': '1', 'X-Username': 'admin', 'X-User-Roles': 'ADMIN' };

export function setup() {
  const eventId = Date.now();
  const r = http.post(`${BASE}/api/admin/tickets`,
    JSON.stringify({ eventId, ticketType: TYPE, totalQuantity: STOCK }),
    { headers: ADMIN });
  const code = safeCode(r);
  if (code !== 200) throw new Error(`stock create failed (code=${code}): ${r.body}`);
  console.log(`setup: eventId=${eventId} stock=${STOCK} endpoint=${ENDPOINT} vus=${VUS} iterations=${ITERATIONS}`);
  return { eventId };
}

export default function (data) {
  const uid = (exec.scenario.iterationInTest % 100000) + 1;
  const res = http.post(`${BASE}${ENDPOINT}`,
    JSON.stringify({ eventId: data.eventId, ticketType: TYPE, quantity: 1 }),
    { headers: { 'Content-Type': 'application/json', 'X-User-Id': String(uid), 'X-Username': `u${uid}`, 'X-User-Roles': 'USER' } });
  reserveDur.add(res.timings.duration);
  const code = safeCode(res);
  if (code === 200) success.add(1);
  else if (code === 400) outOfStock.add(1);   // out of stock = expected business rejection, not an error
  else errors.add(1);
}

export function teardown(data) {
  const r = http.get(`${BASE}/api/tickets/events/${data.eventId}`, { headers: ADMIN });
  let stock = null;
  try { stock = r.json('data')[0]; } catch (e) { /* ignore */ }
  if (!stock) { console.log('teardown: could not read final stock'); return; }
  const committed = stock.reservedQuantity + stock.soldQuantity;
  const oversold = committed > stock.totalQuantity;
  console.log('================= RESULT =================');
  console.log(`final stock: total=${stock.totalQuantity} reserved=${stock.reservedQuantity} sold=${stock.soldQuantity} available=${stock.availableQuantity}`);
  console.log(`OVERSELL CHECK: reserved+sold(${committed}) <= total(${stock.totalQuantity})  =>  ${oversold ? 'FAIL - OVERSOLD!' : 'PASS - no oversell'}`);
  console.log('=========================================');
}

function safeCode(res) {
  try { return res.json('code'); } catch (e) { return res.status; }
}
