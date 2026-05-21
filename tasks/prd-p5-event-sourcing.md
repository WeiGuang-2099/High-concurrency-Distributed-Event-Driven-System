# PRD: P5 - Event Store Service & CQRS

## Introduction

实现事件溯源和 CQRS 模式：append-only 事件存储（MongoDB）、事件投影更新 Redis/MongoDB 查询视图、幂等消费、快照机制、缓存一致性策略。这个服务将整个系统的所有状态变更统一管理，是事件驱动架构的核心基础设施。

## Goals

- 所有业务事件 append-only 写入 MongoDB
- 事件投影实时更新 Redis 缓存和 MongoDB 查询视图
- 幂等消费保证 exactly-once 语义
- 每 100 个事件生成快照加速回放
- 写端消费者主动失效 Redis 缓存保证一致性

## User Stories

### US-001: Kafka consumer for event ingestion
**Description:** As the system, all state changes from Kafka topics must be stored as immutable events so we can reconstruct any past state.

**Acceptance Criteria:**
- [ ] event-store-service subscribes to `auction-events`, `ticket-events`, `order-events` topics
- [ ] Each consumed event stored with: aggregateId, aggregateType, eventType, sequenceNumber, payload (JSON), metadata, timestamp
- [ ] MongoDB unique compound index on `(aggregateId, sequenceNumber)` prevents duplicates
- [ ] Duplicate consumption returns success (idempotent, no error thrown, no duplicate written)
- [ ] Events are never updated or deleted (append-only guarantee)
- [ ] Other services do NOT call event-store REST API; they only publish to Kafka, event-store auto-consumes

### US-002: Event projection for CQRS read models
**Description:** As the system, I need to maintain materialized views from events so queries are fast and don't need to replay history.

**Acceptance Criteria:**
- [ ] `AuctionSummaryProjection`: listens to auction-events, maintains `auction_summary:{auctionId}` in Redis with currentHighestBid, bidderId, status, bidCount
- [ ] `BidHistoryProjection`: listens to auction-events, maintains bid history in MongoDB `bid_history` collection for paginated queries
- [ ] `OrderTimelineProjection`: listens to order-events, maintains order status timeline in MongoDB
- [ ] `StockMovementProjection`: listens to ticket-events, maintains stock movement log in MongoDB
- [ ] Each projection is a separate Kafka consumer with own consumer group

### US-003: Cache invalidation on write
**Description:** As the system, when an event changes state, the corresponding Redis cache must be invalidated so reads always return fresh data.

**Acceptance Criteria:**
- [ ] After processing BidPlaced event: delete `auction_summary:{auctionId}` from Redis
- [ ] After processing StockReserved/StockReleased event: delete `stock:{eventId}:{ticketType}` from Redis (refreshed on next read)
- [ ] After processing OrderCreated event: delete `order:{orderId}` from Redis
- [ ] Cache invalidation happens in the same Kafka consumer as projection update (same transaction boundary)

### US-004: Snapshot generation
**Description:** As the system, I need periodic snapshots of aggregate state so event replay doesn't require reading thousands of events.

**Acceptance Criteria:**
- [ ] After every 100 events for an aggregate: generate snapshot document in MongoDB `snapshots` collection
- [ ] Snapshot contains: aggregateId, aggregateType, sequenceNumber (of last event in snapshot), state (serialized aggregate)
- [ ] On replay: load latest snapshot, then apply only events with sequenceNumber > snapshot.sequenceNumber
- [ ] Snapshot generation is triggered by event-store consumer, not a separate scheduled task

### US-005: Event replay for state reconstruction
**Description:** As a developer, I need to replay events to reconstruct aggregate state so I can debug issues or rebuild read models.

**Acceptance Criteria:**
- [ ] `POST /api/admin/events/replay/{aggregateType}/{aggregateId}` triggers replay
- [ ] Loads latest snapshot, then applies remaining events in sequence order
- [ ] Returns reconstructed aggregate state
- [ ] Can optionally rebuild all read model projections from scratch

### US-006: Query API for read models
**Description:** As a frontend developer, I need query APIs that read from projections so I don't hit the write-side database.

**Acceptance Criteria:**
- [ ] `GET /api/event-store/auctions/{id}/history` returns bid history from MongoDB projection (paginated)
- [ ] `GET /api/event-store/orders/{id}/timeline` returns order status timeline from MongoDB
- [ ] `GET /api/event-store/stock/{eventId}/movements` returns stock movement log from MongoDB
- [ ] Response time < 50ms for projection queries

## Functional Requirements

- FR-1: event-store-service consumes `auction-events`, `ticket-events`, `order-events` Kafka topics and appends to MongoDB
- FR-2: MongoDB unique index on (aggregateId, sequenceNumber) enforces idempotency
- FR-3: Four projection consumers: AuctionSummary, BidHistory, OrderTimeline, StockMovement
- FR-4: Each projection maintains Redis cache and/or MongoDB query collection
- FR-5: Write-side consumer invalidates Redis keys on each event
- FR-6: Snapshot generated every 100 events per aggregate
- FR-7: Event replay supports snapshot + incremental events
- FR-8: Query API reads from projections, never from event stream directly

## Non-Goals

- No event versioning or schema evolution (single event schema version for demo)
- No event upcasting (transforming old events to new format)
- No multi-aggregate transactions (each aggregate has independent event stream)
- No event archiving or cold storage
- No real-time streaming dashboard (deferred to observability phase)

## Technical Considerations

- event-store-service consumes ALL Kafka topics (auction-events, ticket-events, order-events) and writes to MongoDB; no REST ingestion API needed
- Other services do NOT call event-store REST API; they only publish to Kafka, event-store auto-consumes
- MongoDB `events` collection uses TTL index (30 days) to manage storage
- Projection consumers use Kafka consumer groups: `projection-auction-summary`, `projection-bid-history`, etc.
- Cache invalidation uses Redis `DEL` command; next read falls back to MongoDB projection and repopulates cache

## Success Metrics

- Event ingestion latency < 10ms (MongoDB append)
- Projection update latency < 100ms from event publication to cache invalidation
- Event replay reconstructs state within 5 seconds for aggregates with 1000+ events
- Zero duplicate events in MongoDB under concurrent producers
