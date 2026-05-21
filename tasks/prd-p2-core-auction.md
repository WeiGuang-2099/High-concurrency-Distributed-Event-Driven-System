# PRD: P2 - Core Auction Service

## Introduction

实现竞拍核心服务：创建竞拍、实时出价（Redis Lua 原子校验）、Kafka 事件发布、WebSocket 实时推送、竞拍到期自动结算。这是整个项目最核心的服务，也是面试展示的主要技术亮点。

## Goals

- 管理员可以创建竞拍活动（关联票务库存）
- 用户可以实时出价，Redis Lua 脚本保证原子性
- 所有竞拍事件通过 Kafka 异步分发
- notification-service 通过 WebSocket 实时推送出价更新
- 竞拍到期自动结算，选出赢家并触发订单创建
- 同价出价先到先得（时间戳决胜）

## User Stories

### US-001: Create auction (admin API)
**Description:** As an admin, I want to create an auction event so users can bid on items.

**Acceptance Criteria:**
- [ ] `POST /api/admin/auctions` accepts `{eventName, description, startingPrice, ticketTypeId, startTime, endTime}`
- [ ] Validates endTime > startTime > now
- [ ] Creates auction with status `PENDING` (auto-transitions to `ACTIVE` at startTime via scheduler)
- [ ] Publishes `AuctionCreated` event to Kafka `auction-events` topic
- [ ] Returns 201 with created auction details

### US-002: List and query auctions
**Description:** As a user, I want to browse available auctions so I can find something to bid on.

**Acceptance Criteria:**
- [ ] `GET /api/auctions` returns paginated list (default page=0, size=20)
- [ ] `GET /api/auctions/hot` returns top 10 active auctions by bid count (Redis cached, 30s TTL)
- [ ] `GET /api/auctions/{id}` returns full auction detail including current highest bid
- [ ] Response includes: id, eventName, startingPrice, currentHighestBid, currentHighestBidder, status, remaining seconds

### US-003: Place bid with Redis Lua atomic validation
**Description:** As a user, I want to place a bid that is atomically validated so no two users can create conflicts.

**Acceptance Criteria:**
- [ ] `POST /api/auctions/{id}/bids` accepts `{amount}`, requires JWT auth
- [ ] Redis Lua script atomically checks: auction is ACTIVE + bid > currentHighestBid
- [ ] If valid: Lua script updates currentHighestBid + bidder in Redis, returns success
- [ ] If invalid: returns 400 with reason ("Auction not active" / "Bid too low")
- [ ] On success: publishes `BidPlaced` event to Kafka `auction-events` topic
- [ ] On outbid (previous bidder): publishes `BidOutbid` event
- [ ] Response time < 20ms for bid placement (Redis-only fast path)

### US-004: Get bid history
**Description:** As a user, I want to see the bid history of an auction so I can understand the competition.

**Acceptance Criteria:**
- [ ] `GET /api/auctions/{id}/bids` returns paginated bid history (newest first)
- [ ] Each bid entry shows: bidder username (masked email), amount, timestamp
- [ ] Data sourced from MongoDB event projections (not MySQL, to demonstrate CQRS)

### US-005: Auction lifecycle scheduling (activation + expiration)
**Description:** As the system, I need to automatically activate and settle auctions based on their configured times.

**Acceptance Criteria:**
- [ ] Scheduled task polls MySQL every 1 second with two queries:
  - Activation: auctions where `startTime <= NOW()` and `status = PENDING` -> set ACTIVE
  - Expiration: auctions where `endTime <= NOW()` and `status = ACTIVE` -> trigger settlement
- [ ] On activation: publishes `AuctionActivated` event, initializes Redis key `auction:{id}:highest` with startingPrice
- [ ] On expiry: finds highest bid from MySQL `bid` table (not MongoDB -- event-store may not exist yet)
- [ ] If highest bid exists: marks auction as SETTLED, publishes `AuctionSettled` event
- [ ] If no bids: marks auction as EXPIRED, publishes `AuctionExpired` event
- [ ] Backup trigger: RabbitMQ delayed message (TTL = auction duration) as fallback

### US-006: Kafka auction event producer and consumer
**Description:** As the system, I need auction events reliably published and consumed so all services stay in sync.

**Acceptance Criteria:**
- [ ] auction-service produces events to `auction-events` topic (6 partitions)
- [ ] Event payload includes: eventId, eventType, aggregateId, payload (JSON), timestamp, correlationId
- [ ] notification-service consumes and pushes WebSocket updates (built in this phase)
- [ ] event-store-service consumption deferred to P5 (events accumulate in Kafka until P5 builds the consumer)
- [ ] Dead letter topic catches failed consumptions

### US-007: WebSocket real-time bid push
**Description:** As a user viewing an auction, I want to see new bids appear in real-time without refreshing so I can react quickly.

**Acceptance Criteria:**
- [ ] notification-service exposes STOMP endpoint at `/ws/notifications`
- [ ] Clients subscribe to `/topic/auction/{auctionId}` for bid updates
- [ ] On BidPlaced event: WebSocket message pushed to all subscribers of that auction
- [ ] Message contains: bidderName, amount, timestamp
- [ ] WebSocket connection requires valid JWT in CONNECT frame
- [ ] Invalid JWT -> connection rejected

## Functional Requirements

- FR-1: `POST /api/admin/auctions` creates auction with PENDING/ACTIVE status and publishes Kafka event
- FR-2: `GET /api/auctions` returns paginated list; `/hot` returns cached top 10
- FR-3: `POST /api/auctions/{id}/bids` validates bid via Redis Lua, publishes event on success
- FR-4: Redis Lua script: GET current bid -> compare -> SET new bid (all atomic)
- FR-5: Scheduled task polls for both activation (PENDING->ACTIVE) and expiration (ACTIVE->SETTLED/EXPIRED) every 1s
- FR-6: Auction settlement queries MySQL `bid` table for highest bid and publishes AuctionSettled/AuctionExpired event
- FR-7: Kafka producer uses `auction-events` topic with 6 partitions
- FR-8: WebSocket STOMP endpoint authenticates via JWT and pushes bid updates per auction

## Non-Goals

- No Seata distributed transaction in this phase (settlement TX deferred to P4)
- No proxy bidding or auto-bid feature
- No auction image upload
- No anti-snipe extension (extending timer on last-second bids)
- No auction categories or search

## Technical Considerations

- Redis Lua script runs within single Redis thread, guaranteeing atomicity
- Lua script key: `auction:{auctionId}:highest` stores `{bidderId}:{amount}:{timestamp}`
- Bid validation fast path is Redis-only; MySQL update is async via Kafka consumer
- Scheduled task uses `@Scheduled(fixedRate = 1000)` with `idx_auction_status_end_time` index for efficient polling of both activation and expiration
- WebSocket STOMP broker relay can be embedded (simple broker) for demo; production would use external RabbitMQ STOMP plugin

## Success Metrics

- 1000 concurrent bids on single auction with zero oversell/conflicts
- Bid placement latency < 20ms (P99)
- WebSocket push latency < 100ms from bid placement to client receipt
- Auction settlement completes within 2 seconds of expiry
