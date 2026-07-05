// Gateway rate-limit test: proves the per-user Redis token bucket guarding
// POST /api/tickets/reserve at the gateway (:8080).
//
// Two concurrent scenarios, independent users so independent buckets:
//   hammer:    ONE user fires 30 req/s for 20s. With replenish=5/s and burst=10
//              only ~110 requests may pass; the rest must be rejected with 429
//              at the gateway (never reaching ticket-service).
//   compliant: 5 users at ~2 req/s each, below the 5/s budget - zero 429s.
//
// Requires the full stack behind the gateway: eureka-server, config-server,
// gateway-service (:8080), user-service, ticket-service + docker infra.
// Unlike ticket-rush.js (which bypasses the gateway to benchmark the service in
// isolation), this test MUST go through the gateway - that is where the limiter is.
//
// Parameters (via -e KEY=VALUE):
//   GATEWAY_URL  gateway base url                  (default http://localhost:8080)
//   TICKET_URL   ticket-service direct base url    (default http://localhost:8082)
//                (admin stock creation bypasses the gateway, same as ticket-rush.js)
//   STOCK        stock to create                   (default 1000)
//
// Run: k6 run gateway-rate-limit.js

import http from 'k6/http';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

const GW     = __ENV.GATEWAY_URL || 'http://localhost:8080';
const TICKET = __ENV.TICKET_URL || 'http://localhost:8082';
const STOCK  = parseInt(__ENV.STOCK || '1000', 10);
const TYPE   = 'VIP';

const hammerPassed       = new Counter('hammer_passed');        // reached ticket-service
const hammerThrottled    = new Counter('hammer_throttled');     // 429 at the gateway
const compliantThrottled = new Counter('compliant_throttled');
const errors             = new Counter('rl_errors');

export const options = {
  // Same Windows resolver workarounds as ticket-rush.js.
  dns: { ttl: '1h', select: 'first', policy: 'preferIPv4' },
  hosts: {
    'localhost:8080': '127.0.0.1:8080',
    'localhost:8082': '127.0.0.1:8082',
  },
  scenarios: {
    hammer: {
      executor: 'constant-arrival-rate', exec: 'hammer',
      rate: 30, timeUnit: '1s', duration: '20s', preAllocatedVUs: 40,
    },
    compliant: {
      // 10 req/s spread round-robin over 5 users = exactly 2 req/s per user.
      executor: 'constant-arrival-rate', exec: 'compliant',
      rate: 10, timeUnit: '1s', duration: '20s', preAllocatedVUs: 10,
    },
  },
  thresholds: {
    // Token bucket math: burst 10 + replenish 5/s x 20s ~= 110 passes; allow jitter.
    hammer_passed:       ['count>=95', 'count<=135'],
    hammer_throttled:    ['count>300'],
    compliant_throttled: ['count<1'],
    rl_errors:           ['count<1'],
  },
};

const JSON_HEADERS = { 'Content-Type': 'application/json' };
const ADMIN = { ...JSON_HEADERS, 'X-User-Id': '1', 'X-Username': 'admin', 'X-User-Roles': 'ADMIN' };

export function setup() {
  const stamp = Date.now();
  const hammerToken = registerAndLogin(`rl_hammer_${stamp}`);
  const compliantTokens = [];
  for (let i = 0; i < 5; i++) {
    compliantTokens.push(registerAndLogin(`rl_quiet_${stamp}_${i}`));
  }

  const eventId = stamp;
  const r = http.post(`${TICKET}/api/admin/tickets`,
    JSON.stringify({ eventId, ticketType: TYPE, totalQuantity: STOCK }),
    { headers: ADMIN });
  if (safeCode(r) !== 200) throw new Error(`stock create failed: ${r.status} ${r.body}`);
  console.log(`setup: eventId=${eventId} stock=${STOCK} gateway=${GW}`);
  return { hammerToken, compliantTokens, eventId };
}

export function hammer(data) {
  const res = reserve(data, data.hammerToken);
  if (res.status === 429) hammerThrottled.add(1);
  else if (passedLimiter(res)) hammerPassed.add(1);
  else errors.add(1);
}

export function compliant(data) {
  // Round-robin by iteration number: every user gets an even 1/5 share of the
  // 10 req/s arrival rate, i.e. ~2 req/s each - below the 5/s per-user budget.
  const token = data.compliantTokens[exec.scenario.iterationInTest % data.compliantTokens.length];
  const res = reserve(data, token);
  if (res.status === 429) compliantThrottled.add(1);
  else if (!passedLimiter(res)) errors.add(1);
}

function reserve(data, token) {
  return http.post(`${GW}/api/tickets/reserve`,
    JSON.stringify({ eventId: data.eventId, ticketType: TYPE, quantity: 1 }),
    { headers: { ...JSON_HEADERS, Authorization: `Bearer ${token}` } });
}

// 200 = reserved; 400 = out-of-stock business rejection. Both mean the request got
// PAST the gateway limiter, which is all this test measures.
function passedLimiter(res) {
  return res.status === 200 || res.status === 400;
}

function registerAndLogin(username) {
  const password = 'LoadTest123!';
  let r = http.post(`${GW}/api/users/register`,
    JSON.stringify({ username, email: `${username}@test.local`, password }),
    { headers: JSON_HEADERS });
  if (r.status !== 201 && safeCode(r) !== 200) {
    throw new Error(`register failed for ${username}: ${r.status} ${r.body}`);
  }
  r = http.post(`${GW}/api/users/login`,
    JSON.stringify({ username, password }), { headers: JSON_HEADERS });
  let token = null;
  try { token = r.json('data.token'); } catch (e) { /* fall through */ }
  if (!token) throw new Error(`login failed for ${username}: ${r.status} ${r.body}`);
  return token;
}

function safeCode(res) {
  try { return res.json('code'); } catch (e) { return res.status; }
}
