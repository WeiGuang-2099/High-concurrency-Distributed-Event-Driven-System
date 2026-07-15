# 1. Record architecture decisions

Date: 2026-07-15
Status: Accepted

## Context

The interesting decisions in this project were the ones that got reversed, and the reasoning
for them lived only in commit messages and javadoc. "Why is there no Seata here when the README
says distributed transactions?" and "why is a bid accepted by Redis rather than MySQL?" both have
real answers backed by measurements, but nothing collected them in one place.

## Decision

Record the decisions that shaped the architecture as ADRs in `docs/adr/`, one file per decision,
in the Nygard format (Context / Decision / Consequences).

Only decisions with a genuine alternative get an ADR. "Use Spring Boot" is not a decision, it is
the starting point. A decision qualifies when picking differently would have produced a
materially different system, and especially when the reasoning is not visible from the code that
resulted — the absence of Seata is invisible; the presence of a Lua script is not self-explaining.

Where a decision was driven by a measurement, the ADR cites the number and where it came from,
so the claim can be re-checked rather than believed.

## Consequences

- Reversals are cheap to explain: an ADR gets a `Superseded by` line rather than being edited to
  pretend the earlier decision never happened.
- The ADRs are the honest record, including the parts that did not work out — see ADR-0003, where
  the first choice (Seata AT) was abandoned after it turned out to cost more than it bought.
- Cost: a decision made and not recorded leaves the set misleading. That is the failure mode to
  watch for.
