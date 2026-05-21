# PRD: P4 - Order Service & Distributed Transactions

## Introduction

实现订单服务和分布式事务协调：订单创建与状态机流转、Mock 支付服务、Seata AT 模式保证跨服务事务一致性、超时回滚机制。这个阶段将 auction-service、ticket-service、order-service 三个服务的事务串联起来。

## Goals

- 竞拍结算和票务购买都能创建订单
- 订单状态机清晰流转：Created -> Paying -> Paid -> Completed / Cancelled
- Mock 支付服务模拟真实支付网关行为
- Seata AT 模式保证竞拍结算链路的强一致性
- RabbitMQ 延迟消息保证订单超时自动取消并释放库存

## User Stories

### US-001: Create order from ticket purchase
**Description:** As a user, I want my ticket reservation to generate an order so I can proceed to payment.

**Acceptance Criteria:**
- [ ] After `POST /api/tickets/reserve` succeeds, frontend calls `POST /api/orders` with `{reservationId, type: "TICKET"}`
- [ ] order-service creates order with status CREATED, amount from reservation
- [ ] Returns orderId for payment initiation
- [ ] Publishes `OrderCreated` event to Kafka `order-events` topic

### US-002: Create order from auction settlement
**Description:** As the system, when an auction settles, I need to automatically create an order for the winning bidder.

**Acceptance Criteria:**
- [ ] auction-service calls order-service via OpenFeign on settlement
- [ ] `POST /api/orders/internal/auction` (internal endpoint, not exposed via gateway) accepts `{auctionId, winnerId, amount}`
- [ ] order-service creates order with type AUCTION and status CREATED
- [ ] Publishes `OrderCreated` event to Kafka

### US-003: Order state machine
**Description:** As the system, I need orders to follow a strict state transition flow so payment status is always consistent.

**Acceptance Criteria:**
- [ ] States: CREATED -> PAYING -> PAID -> COMPLETED / CANCELLED / EXPIRED
- [ ] `POST /api/orders/{id}/pay` transitions CREATED -> PAYING (initiates mock payment)
- [ ] Repeated pay call when already PAYING: returns 200 with current state (idempotent)
- [ ] Repeated pay call when already PAID: returns 200 with current state (idempotent)
- [ ] Mock payment callback transitions PAYING -> PAID
- [ ] On payment confirmed: order-service calls ticket-service to confirm reservation
- [ ] On payment confirmed: transitions PAID -> COMPLETED
- [ ] Invalid transitions return 409 with current state and attempted transition

### US-004: Mock payment service
**Description:** As a developer, I need a mock payment provider that simulates real gateway behavior so the payment flow is testable without external dependencies.

**Acceptance Criteria:**
- [ ] `POST /api/orders/{id}/pay` triggers mock payment
- [ ] Mock payment controller simulates gateway with configurable success rate (default 95%)
- [ ] Simulates processing latency (random 500-2000ms)
- [ ] 5% of payments randomly fail (simulate payment rejection)
- [ ] Success: transitions order to PAID and calls ticket confirm
- [ ] Failure: transitions order back to CREATED (user can retry)

### US-005: Seata distributed transaction for auction settlement
**Description:** As the system, auction settlement must atomically reserve stock and create order so no partial state occurs.

**Acceptance Criteria:**
- [ ] auction-service method annotated with `@GlobalTransactional`
- [ ] Within TX: OpenFeign call to ticket-service `/api/tickets/internal/settle-reserve` (MySQL-only, no Redis) reserves stock (Seata RM)
- [ ] Within TX: OpenFeign call to order-service `/api/orders/internal/auction` creates order (Seata RM)
- [ ] All succeed: Seata TC commits both operations; async Kafka consumer syncs Redis stock from MySQL state
- [ ] Any fails: Seata TC rolls back both MySQL operations via undo_log (Redis never touched, so no compensation needed)
- [ ] Seata `undo_log` table exists in `auction_db`, `ticket_db`, `order_db`

### US-006: Order timeout and cancellation
**Description:** As the system, unpaid orders should be automatically cancelled after 30 minutes and stock released.

**Acceptance Criteria:**
- [ ] On order creation: RabbitMQ delayed message sent with 30-min TTL
- [ ] order-service consumes delayed message: checks order status
- [ ] If still CREATED/PAYING (not yet PAID): cancels order, calls ticket-service to release stock
- [ ] Publishes `OrderExpired` or `OrderCancelled` event to Kafka
- [ ] If already COMPLETED: ignores message (idempotent)

### US-007: Query orders
**Description:** As a user, I want to view my orders so I can track their status.

**Acceptance Criteria:**
- [ ] `GET /api/orders?userId={userId}` returns paginated list of user's orders
- [ ] `GET /api/orders/{id}` returns full order detail with status history
- [ ] Only returns orders belonging to authenticated user (enforced by X-User-Id header)

## Functional Requirements

- FR-1: `POST /api/orders` creates order from ticket reservation or auction settlement
- FR-2: Order state machine enforces valid transitions only
- FR-3: Mock payment simulates success/failure with configurable rates
- FR-4: Seata AT mode coordinates auction -> ticket -> order transaction
- FR-5: Each participating service has `undo_log` table for Seata rollback
- FR-6: OpenFeign clients carry Seata XID in request headers for TX propagation
- FR-7: RabbitMQ delayed message triggers order cancellation on timeout
- FR-8: Order cancellation releases ticket stock via ticket-service API
- FR-9: Idempotent Kafka consumption via `idempotency_log` table

## Non-Goals

- No real payment gateway integration (Stripe, PayPal, etc.)
- No refund flow
- No partial payments or installments
- No invoice generation
- No order splitting or merging

## Technical Considerations

- Seata AT mode requires `undo_log` table in auction_db, ticket_db, order_db with this schema:
  ```sql
  CREATE TABLE undo_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    branch_id BIGINT NOT NULL,
    xid VARCHAR(100) NOT NULL,
    context VARCHAR(128) NOT NULL,
    rollback_info LONGBLOB NOT NULL,
    log_status INT NOT NULL,
    log_created DATETIME NOT NULL,
    log_modified DATETIME NOT NULL,
    UNIQUE KEY ux_undo_log (xid, branch_id)
  );
  ```
- SeataFeignInterceptor implementation: adds `RootContext.getXID()` as header `TX_XID` on all outgoing Feign requests
- **Why MySQL-only for settlement**: Seata AT mode uses undo_log to reverse MySQL changes on rollback. It cannot reverse Redis operations. The settlement path uses `settle-reserve` (MySQL-only) so Seata has full rollback control. Redis stock stays consistent because: (1) settlement is a different flow from direct purchase; (2) the Kafka `StockReserved` / `StockReleased` events trigger async Redis sync in ticket-service.
- Seata config: transaction group `auction-tx-group`, mapped to Seata Server cluster in Nacos config `service.vgroupMapping.auction-tx-group=default`
- Mock payment is synchronous: `POST /api/orders/{id}/pay` -> `Thread.sleep(500-2000ms)` -> returns success/failure in same HTTP response
- Mock payment latency configurable via Nacos property `payment.mock.latency-ms`
- RabbitMQ delayed message for order timeout reuses the same `delay.exchange` from P3
- Order state machine uses simple enum-based switch (not Spring Statemachine -- overkill for 6 states)

## Success Metrics

- Auction settlement Seata TX completes in < 3 seconds end-to-end
- Forced failure during settlement: both stock reservation and order creation roll back cleanly
- Order timeout triggers stock release within 5 seconds of TTL expiry
- Mock payment success rate matches configured percentage
