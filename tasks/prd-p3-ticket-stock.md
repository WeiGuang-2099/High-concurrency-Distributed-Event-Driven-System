# PRD: P3 - Ticket Service & Oversell Prevention

## Introduction

实现票务库存服务：Redis Lua 原子预扣减、超卖防护、30 分钟库存预留超时机制、RabbitMQ 延迟队列触发回滚。这个服务展示的核心面试亮点是"如何在 1 万并发下保证不超卖不少卖"。

## Goals

- 票务库存通过 Redis Lua 脚本实现原子预扣减，杜绝超卖
- 库存预留 30 分钟超时自动释放（RabbitMQ 延迟消息）
- 预扣减成功后异步同步到 MySQL（最终一致性）
- Redis 宕机时从 MySQL 重建库存缓存

## User Stories

### US-001: Create ticket stock batch (admin)
**Description:** As an admin, I want to create a batch of tickets for an event so users can purchase them.

**Acceptance Criteria:**
- [ ] `POST /api/admin/tickets` accepts `{eventId, ticketType, totalQuantity, price}`
- [ ] Creates `ticket_stock` record in MySQL
- [ ] Initializes Redis key `stock:{eventId}:{ticketType}` with totalQuantity
- [ ] Publishes `TicketCreated` event to Kafka `ticket-events` topic
- [ ] Returns 201 with stock details

### US-002: Get ticket availability
**Description:** As a user, I want to see how many tickets are available for an event so I know if I can buy one.

**Acceptance Criteria:**
- [ ] `GET /api/tickets/events/{eventId}` returns all ticket types with availability
- [ ] Available count = totalQuantity - reservedQuantity - soldQuantity (served from Redis cache)
- [ ] Returns: ticketType, price, totalQuantity, availableCount

### US-003: Reserve ticket with Redis Lua atomic pre-deduction
**Description:** As a user, I want to reserve a ticket so I can proceed to payment. The system must guarantee no overselling.

**Acceptance Criteria:**
- [ ] `POST /api/tickets/reserve` accepts `{eventId, ticketType, quantity}`, requires JWT
- [ ] Redis Lua script atomically: GET `stock:{eventId}:{ticketType}` -> if result >= quantity then DECRBY, return success
- [ ] If stock insufficient: returns 409 "Out of stock" (no Redis state change)
- [ ] On success: creates `reservation` record in MySQL with status RESERVED and expire_at = now + 30min
- [ ] If MySQL insert fails after Redis deduction: catch exception, execute Redis INCRBY to rollback stock, return 500
- [ ] Publishes `StockReserved` event to Kafka `ticket-events` topic
- [ ] Sends delayed message to RabbitMQ `delay.exchange` with 30-min TTL for auto-release
- [ ] Returns reservationId for subsequent payment flow

### US-004: Confirm reservation after payment
**Description:** As the system, I need to confirm a reservation when payment succeeds so the ticket is permanently allocated.

**Acceptance Criteria:**
- [ ] `POST /api/tickets/{reservationId}/confirm` called by order-service after payment success
- [ ] Updates reservation status from RESERVED to CONFIRMED in MySQL
- [ ] Updates `ticket_stock.soldQuantity` in MySQL
- [ ] Publishes `StockConfirmed` event to Kafka
- [ ] Cancels the RabbitMQ delayed release message (or ignores if already delivered)

### US-005: Cancel reservation and release stock
**Description:** As the system, I need to release reserved stock when a user cancels or the reservation expires.

**Acceptance Criteria:**
- [ ] `DELETE /api/tickets/{reservationId}` releases reserved stock
- [ ] Redis INCRBY restores the deducted quantity
- [ ] Updates reservation status to CANCELLED in MySQL
- [ ] Publishes `StockReleased` event to Kafka

### US-006: Auto-release expired reservations via RabbitMQ delayed queue
**Description:** As the system, I need to automatically release stock when a reservation times out after 30 minutes so other users can buy.

**Acceptance Criteria:**
- [ ] RabbitMQ delayed message plugin configured with `delay.exchange` (x-delayed-type: direct)
- [ ] On reservation: message sent with x-delay = 30 minutes, routing to `stock-release-queue`
- [ ] ticket-service consumes `stock-release-queue`: checks reservation status
- [ ] If still RESERVED (no payment): releases stock in Redis + MySQL, marks as EXPIRED
- [ ] If already CONFIRMED/CANCELLED: ignores message (idempotent)

### US-007: Redis stock cache rebuild on startup
**Description:** As the system, I need to rebuild Redis stock cache from MySQL when Redis restarts so inventory data is not lost.

**Acceptance Criteria:**
- [ ] On ticket-service startup: `ApplicationRunner` loads all `ticket_stock` records from MySQL
- [ ] For each stock: computes available = totalQuantity - reservedQuantity - soldQuantity
- [ ] Sets `stock:{eventId}:{ticketType}` = available in Redis
- [ ] During rebuild: `/api/tickets/reserve` returns 503 with "Service initializing"
- [ ] Rebuild completion logged and flag set in Redis `stock:initialized`

## Functional Requirements

- FR-1: `POST /api/admin/tickets` creates ticket stock in MySQL and initializes Redis cache
- FR-2: `GET /api/tickets/events/{eventId}` returns availability from Redis cache
- FR-3: `POST /api/tickets/reserve` executes Redis Lua atomic pre-deduction, creates reservation, sends delayed message
- FR-4: `POST /api/tickets/{reservationId}/confirm` finalizes reservation after payment
- FR-5: `DELETE /api/tickets/{reservationId}` releases stock back to pool
- FR-6: RabbitMQ delayed consumer auto-releases expired reservations
- FR-7: Startup listener rebuilds Redis cache from MySQL
- FR-8: All stock mutations publish events to Kafka `ticket-events` topic
- FR-9: `POST /api/tickets/internal/settle-reserve` provides MySQL-only reservation for Seata 2PC auction settlement

## Non-Goals

- No seat selection (general admission only, ticket-type based)
- No ticket QR code generation
- No ticket transfer between users
- No dynamic pricing

### US-008: Reserve stock for auction settlement (internal, MySQL-only)
**Description:** As the system, auction settlement via Seata 2PC needs a MySQL-only stock reservation endpoint so Seata's undo_log can correctly roll back on failure.

**Acceptance Criteria:**
- [ ] `POST /api/tickets/internal/settle-reserve` (internal endpoint, not exposed via gateway) accepts `{eventId, ticketType, quantity, auctionId}`
- [ ] Checks and deducts stock in MySQL only (updates `ticket_stock.reservedQuantity` with optimistic lock on version column)
- [ ] Does NOT touch Redis (Seata undo_log cannot compensate Redis state)
- [ ] Creates `reservation` record with status RESERVED and source = AUCTION_SETTLEMENT
- [ ] Publishes `StockReserved` event to Kafka `ticket-events` topic
- [ ] On Seata rollback: undo_log automatically restores MySQL state; Redis sync happens asynchronously via Kafka consumer when StockReleased event is published

## Technical Considerations

- Redis Lua script uses `redis.call('GET', KEYS[1])` then conditional `redis.call('DECRBY', KEYS[1], ARGV[1])`
- Redis deduction and MySQL insert are in the same method with try-catch; on MySQL failure, Redis is compensated immediately via INCRBY
- Lua scripts stored in `deploy/redis/lua/` and loaded via Spring's `DefaultRedisScript`
- RabbitMQ delayed message plugin: `rabbitmq-plugins enable rabbitmq_delayed_message_exchange`
- Delayed message uses `x-delayed-message` exchange type with `x-delay` header in milliseconds
- MySQL `ticket_stock.version` column used only as a safety net during Redis cache rebuild or when Redis is unavailable

## Success Metrics

- 5000 concurrent ticket reservations for 1000 available tickets: exactly 1000 succeed, 4000 fail
- Zero oversell under any concurrency level
- Expired reservation stock released within 5 seconds of TTL expiry
- Redis rebuild completes in < 10 seconds for 100 event types
