# 8. The gateway routes an allow-list and refuses paths that are not canonical

Date: 2026-11-12
Status: Accepted

## Context

The gateway forwarded each service wholesale (`Path=/api/tickets/**` and so on) and kept the
service-to-service endpoints out with one filter that looked for a path segment equal to
`internal`. That is a deny-list: everything a service maps is public unless someone thought to
forbid it.

A review in October 2026, followed by probes sent to the running gateway, found it wrong in five
places. Each needed nothing more than an ordinary user's token.

- `/api/tickets/internal;x/settle-reserve` reached the settlement handler. The filter compared
  the segment `internal;x` with `internal`. Spring MVC drops `;x` before matching, and so does
  the gateway's own `Path` predicate. The handler takes the winner's user id and the quantity
  from the request body.
- The discovery locator was on, so every registered service was also published as
  `/{service-id}/**`. Those routes carry no rate limiter. They were also the only gateway path to
  `event-store-service`, whose endpoints check nothing (any order's timeline, unmasked bidder
  names, an "admin" replay with no role check), and to each service's actuator.
- `/api/tickets//reserve` was not rate limited. The limited route's exact pattern does not match
  the empty segment, the wildcard route does, and the gateway deletes empty segments when it
  builds the outgoing URL (`UriComponentsBuilder`, inside `RouteToRequestUrlFilter`), which is
  after routing. Thirty rapid requests at the shipped limits: 30 passed on that path, 18 to 20 on
  the canonical one.
- `POST /api/tickets/{id}/confirm` and the benchmark endpoint
  `POST /api/tickets/reserve-pessimistic` were public because the wildcard covered them. The
  first turns the caller's own pending reservation into a sold ticket with no payment.
- `POST /api/admin/auctions` had no route at all, so the admin page could not create an auction.

Two facts about the framework shaped the fix. A `GlobalFilter` runs only after a route has
matched, so the internal-path filter never saw a request that matched no route and would have
been dead code under an allow-list. And the existing tests could not see any of the above:
`GatewayRateLimitIT` switched the locator off in its own properties, and
`WebTestClient.uri(String)` collapses `//` and encodes `%` again before sending, so the obvious
test for a non-canonical path sends a canonical one and passes.

The first alternative was to keep the wildcard routes and repair the filter: normalise the path,
then look for `internal`. That keeps the deny-list. It also makes the gateway a third parser that
has to agree with Reactor Netty in front of it and with Tomcat and Spring MVC behind it, and the
probes showed how little those agree. Spring MVC decodes a segment and strips its parameters but
does not resolve `..`. Tomcat rejects `%2F` and `%00`, except inside a path parameter, where it
lets them through. The gateway collapses `//`, but only on the way out.

## Decision

1. **Refuse, do not normalise.** `CanonicalPathFilter`, a `WebFilter` that runs first, answers
   400 unless the raw path is `/` followed by segments of unreserved characters
   (`A-Z a-z 0-9 - . _ ~`), with no empty segment and no `.` or `..` segment. That rules out `;`,
   every percent-encoding and a trailing slash. A canonical path is one that every parser in the
   chain reads the same way, and nothing this API serves needs more than that.
2. **The route table is the list.** `application.yml` names every (method, exact path) pair. No
   `*`, no discovery locator, numeric ids written `{id:[0-9]+}`. A request that matches nothing
   is 404.
3. **Public is declared per route.** A route needs a JWT unless it says `metadata.public: true`.
   The exemption used to be decided inside `JwtAuthenticationFilter` from path prefixes, which
   was a second list and defaulted to open. The identity headers (`X-User-Id`, `X-Username`,
   `X-User-Roles`) now only ever come from a verified token: overwritten on authenticated routes,
   removed on public ones.
4. **The internal-segment block stays as a second lock.** It is now a `WebFilter` and compares
   the decoded, parameter-stripped segment, which is the value the path matcher itself uses.
5. **A client cannot have a forwarded header removed.** A proxy drops the headers a request
   names in its `Connection` header, and the gateway does so when it builds the outgoing request,
   after every filter has run. With the rest of this change in place, a valid token plus
   `Connection: X-User-Id, X-User-Roles` still reached the service with no identity at all.
   `ConnectionHeaderFilter` cuts the header down to `close`, `keep-alive` and `upgrade`.
6. **Left off the list on purpose:** the internal endpoints; `POST /api/tickets/{id}/confirm`
   (order-service calls it after payment, service to service); `DELETE /api/tickets/{id}` (a
   reservation is released by cancelling its order, and calling it directly also refunds Redis
   stock that a settlement reservation never took); `reserve-pessimistic`; and the event-store
   API, until it has authorisation of its own.

## Consequences

- `GatewayAllowListIT` boots the gateway on the shipped configuration and holds a second copy of
  the list, so changing the public surface means changing the test. It sends requests over a raw
  socket. Run against the old gateway, 88 of the 154 cases it then had failed.
- Unlisted paths return 404, not 503. `FallbackHandler` used to turn every error into "Service
  temporarily unavailable" and log a stack trace. Under an allow-list that would have reported
  every probe as an outage.
- Every route now has a `Method` predicate, so a CORS preflight matches none of them.
  `globalcors.add-to-simple-url-handler-mapping` answers preflights. Without it a cross-origin
  browser gets a bare 403, which the same-origin dev proxy would never have shown.
- A regex containing a comma, such as `[0-9]{1,18}`, cannot be written in the shortcut route
  syntax: the comma splits the pattern and the gateway refuses to start. Ids are `[0-9]+`, so an
  id too long for a `Long` passes the gateway and fails in the service.
- Config-server values override the gateway's own file, so a `spring.cloud.gateway.*` key in
  `config-repo/` could put the locator back. The integration tests run without the config
  server, so `GatewayAllowListIT` imports the two files the gateway would be served, at the same
  precedence, and `ConfigRepoLeavesRoutingAloneTest` names the offending key. The first version
  of that second test could not see the case it was written for: it read
  `Properties.stringPropertyNames()`, which skips values that are not strings, and YAML `true`
  is a Boolean. Reverting each fix in turn and checking that a test failed is what found it.
- The event-store query and replay endpoints are no longer reachable through the gateway. They
  only ever were through the locator. They remain on the service port.
- **What this does not do.** The gateway is the only component that checks who is calling. A
  service believes `X-User-Id` from anyone who can reach its port, and the internal endpoints
  need no header at all, so the service ports must not be reachable from outside. Closing that
  needs a credential between services, which is not built. And closing public confirm does not
  make "no ticket without paying" hold end to end: `POST /api/orders` still takes the amount from
  the client.
