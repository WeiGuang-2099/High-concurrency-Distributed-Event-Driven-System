# Architecture Decision Records

Decisions that shaped this system, in [Nygard format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).
Only decisions with a real alternative are recorded — see [ADR-0001](0001-record-architecture-decisions.md)
for what qualifies. Where a decision rests on a measurement, the ADR cites the number so it can be
re-checked rather than believed.

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-redis-lua-for-oversell-protection.md) | Guard limited stock with a Redis Lua script, not a database lock | Accepted |
| [0003](0003-replace-seata-with-compensation-saga.md) | Replace Seata AT with an application-level compensation saga | Accepted (supersedes Seata AT) |
| [0004](0004-eureka-and-config-server-over-nacos.md) | Use Eureka + Spring Cloud Config instead of Nacos | Accepted (supersedes Nacos) |
| [0005](0005-redis-stream-outbox-for-bid-durability.md) | Make accepted bids durable with a Redis Stream outbox | Accepted |
| [0006](0006-dead-letter-rather-than-skip.md) | A consumer that cannot process an event must dead-letter it, never skip it | Accepted |

Two of the six record a reversal (0003, 0004), and two record a bug class that survived a first
fix (0005, 0006). That is the point of keeping them: the reasoning that led somewhere wrong is
worth more than a tidy story.
