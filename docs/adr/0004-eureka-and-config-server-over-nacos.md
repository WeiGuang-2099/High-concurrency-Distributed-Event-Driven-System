# 4. Use Eureka + Spring Cloud Config instead of Nacos

Date: 2026-07-06 (recorded retrospectively 2026-07-15)
Status: Accepted — supersedes the original Nacos design

## Context

The project started on Spring Cloud Alibaba: Nacos for both service discovery and centralized
config, one server doing both jobs. That is the default in the Chinese market this stack came
from, and it works.

The reason to revisit was audience, not technology. This is a portfolio project, and its target
readers (Australian teams) mostly do not run Nacos. A reviewer reading `bootstrap.yml` with
`spring.cloud.nacos.config.shared-configs` has to take on faith that the author understands
discovery and centralized config, because the concepts are hidden behind an unfamiliar product.
The same reviewer reads a Eureka registration and a Config Server `spring.config.import` without
stopping.

Nacos also cost real debugging time that was specific to it rather than to the concepts: shared
configs pinned to Docker hostnames, `spring-cloud-starter-bootstrap` missing, and services
registering a Docker vEthernet IP that the host-run gateway could not reach.

## Decision

Migrate to Netflix Eureka (`:8761`) for discovery and Spring Cloud Config Server (`:8888`) for
configuration, serving `config-repo/` with the native backend locally and a git backend in the
cloud profile.

`bootstrap.yml` is deleted everywhere; services use `spring.config.import: optional:configserver:`
so they still boot on local defaults when the config server is down.

## Decision criteria, stated honestly

This is a legibility decision, not a technical one. Nacos is a fine product and arguably does more
for less operational surface (one server rather than two). If this system were being deployed to a
team that already runs Nacos, the right call would be to keep it.

## Consequences

- Two servers to run where there was one. Accepted deliberately.
- Verified after the migration (`f2a017c`): 11-module `verify` green; 7 services registered in
  Eureka within ~10 s; gateway register -> login -> reserve returns 200 and internal endpoints
  still 403; the oversell k6 run reproduces exactly 100/2000 successes with 0 errors.
  *Correction, 2026-11-12:* "internal endpoints still 403" was checked with the plain path only.
  `/api/tickets/internal;x/settle-reserve` was forwarded, and the discovery locator, enabled
  since the first commit and carried through this migration, published every service as
  `/{service-id}/**` around the rate limiters. Both are closed by
  [ADR-0008](0008-gateway-allow-list-and-canonical-paths.md).
- The loopback pin that stopped services registering an unreachable virtual-adapter IP survived
  the move; it is now `eureka.instance.ip-address` rather than the Nacos equivalent. The
  underlying problem was never Nacos — it was a host with several virtual adapters, and it would
  have bitten Eureka identically.
