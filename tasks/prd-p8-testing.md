# PRD: P8 - Testing (Unit + Integration + Load)

## Introduction

实现三层测试体系：单元测试（JUnit 5 + Mockito）、集成测试（Testcontainers）、压测（K6）。压测报告是这个求职项目的核心交付物之一，用数据证明高并发能力比代码本身更有说服力。

## Goals

- 核心业务逻辑单元测试覆盖率 70%+
- 集成测试验证 Kafka 消息端到端流转、Seata 事务提交/回滚、Redis Lua 脚本执行
- K6 压测生成三个场景的报告，数据可用于面试展示和 GitHub README
- 压测结果保存为 JSON 报告 + 截图

## User Stories

### US-001: Unit tests for auction-service core logic
**Description:** As a developer, I need unit tests for bid validation and auction lifecycle so I can refactor with confidence.

**Acceptance Criteria:**
- [ ] Test Redis Lua bid validation: higher bid accepted, lower/equal bid rejected, auction not active rejected
- [ ] Test auction state transitions: PENDING -> ACTIVE -> SETTLED / EXPIRED
- [ ] Test settlement logic: highest bidder wins, no-bid scenario handled
- [ ] Mock Redis, Kafka producer, and OpenFeign clients
- [ ] Line coverage >= 70% for auction-service service layer

### US-002: Unit tests for ticket-service oversell prevention
**Description:** As a developer, I need unit tests for stock deduction logic so oversell is caught before production.

**Acceptance Criteria:**
- [ ] Test Lua pre-deduction: stock available -> success, stock insufficient -> failure, exact stock -> success then failure
- [ ] Test reservation lifecycle: RESERVED -> CONFIRMED, RESERVED -> CANCELLED, RESERVED -> EXPIRED
- [ ] Test Redis cache rebuild from MySQL
- [ ] Test concurrent deduction: 10 threads deducting from stock of 5 -> exactly 5 succeed
- [ ] Line coverage >= 70% for ticket-service service layer

### US-003: Unit tests for order-service state machine
**Description:** As a developer, I need unit tests for order state transitions so invalid transitions are caught.

**Acceptance Criteria:**
- [ ] Test valid transitions: CREATED->PAYING->PAID->COMPLETED, CREATED->CANCELLED, PAYING->CANCELLED
- [ ] Test invalid transitions: COMPLETED->CREATED (rejected), PAID->CREATED (rejected)
- [ ] Test mock payment: success path and failure path
- [ ] Line coverage >= 70% for order-service service layer

### US-004: Integration test - Kafka end-to-end event flow
**Description:** As a developer, I need integration tests that verify Kafka events flow from producer to consumer.

**Acceptance Criteria:**
- [ ] Testcontainers starts Kafka, MongoDB, and MySQL containers
- [ ] Test: auction-service publishes BidPlaced event -> event-store-service consumes and writes to MongoDB
- [ ] Test: ticket-service publishes StockReserved event -> order-service consumes and creates order
- [ ] Verify event payload integrity (no data loss in serialization)
- [ ] Tests run in < 30 seconds each

### US-005: Integration test - Seata distributed transaction
**Description:** As a developer, I need integration tests that verify Seata TX commit and rollback.

**Acceptance Criteria:**
- [ ] Testcontainers starts MySQL (with Seata tables), Seata Server
- [ ] Test happy path: auction settlement commits both stock reservation and order creation
- [ ] Test failure path: force exception after stock reservation -> both operations roll back
- [ ] Verify undo_log tables are cleaned up after TX completion
- [ ] Verify MySQL data is in correct state after both commit and rollback

### US-006: Integration test - Redis Lua script execution
**Description:** As a developer, I need integration tests that verify Lua scripts execute correctly against real Redis.

**Acceptance Criteria:**
- [ ] Testcontainers starts Redis container
- [ ] Test bid Lua script with concurrent access (10 concurrent test threads via ExecutorService)
- [ ] Test stock Lua script with exact-boundary cases (stock = 1, stock = 0)
- [ ] Verify atomicity: no race conditions under concurrent execution

### US-007: K6 load test - Flash auction scenario
**Description:** As a developer, I need a load test that simulates 1000 users bidding simultaneously to prove no oversell.

**Acceptance Criteria:**
- [ ] K6 script in `deploy/k6/flash-auction.js`
- [ ] Scenario: 1000 VUs, each places 1 bid on same auction within 10 seconds
- [ ] Assertions: zero HTTP 5xx errors, all bids processed, highest bid wins
- [ ] Output: summary report with P50/P95/P99 latency, RPS, error rate
- [ ] Report saved as `deploy/k6/reports/flash-auction-summary.json`

### US-008: K6 load test - Ticket rush scenario
**Description:** As a developer, I need a load test that simulates 5000 users buying 1000 tickets to prove oversell prevention.

**Acceptance Criteria:**
- [ ] K6 script in `deploy/k6/ticket-rush.js`
- [ ] Scenario: 5000 VUs, each tries to buy 1 ticket, only 1000 available
- [ ] Assertions: exactly 1000 successful reservations, 4000 "out of stock" responses, zero oversell
- [ ] Output: summary report with success/failure breakdown
- [ ] Report saved as `deploy/k6/reports/ticket-rush-summary.json`

### US-009: K6 load test - Mixed workload scenario
**Description:** As a developer, I need a load test with mixed traffic to show the system handles diverse concurrent operations.

**Acceptance Criteria:**
- [ ] K6 script in `deploy/k6/mixed-workload.js`
- [ ] Scenario: 60% browsing (GET auctions), 20% bidding (POST bids), 15% buying tickets, 5% admin operations
- [ ] Ramp: 0 -> 2000 VUs over 60 seconds, hold 60 seconds, ramp down
- [ ] Assertions: P99 < 500ms, error rate < 1%
- [ ] Report saved as `deploy/k6/reports/mixed-workload-summary.json`

## Functional Requirements

- FR-1: Unit tests for auction bid validation, auction lifecycle, stock deduction, order state machine
- FR-2: Unit test line coverage >= 70% for service layers of auction, ticket, order services
- FR-3: Integration tests with Testcontainers for Kafka E2E, Seata TX, Redis Lua
- FR-4: K6 load test: flash auction (1000 concurrent bidders)
- FR-5: K6 load test: ticket rush (5000 concurrent buyers, 1000 stock)
- FR-6: K6 load test: mixed workload (browsing + bidding + buying)
- FR-7: All K6 reports saved as JSON files in `deploy/k6/reports/`

## Non-Goals

- No E2E tests for frontend (manual browser verification only)
- No chaos engineering (random container kills, network partitions)
- No performance tuning based on load test results (this phase just generates reports)
- No CI/CD integration for test automation
- No 24-hour soak tests

## Technical Considerations

- Testcontainers: use `@Testcontainers` and `@Container` annotations with Spring Boot test
- K6 scripts use `http.get()` / `http.post()` for HTTP requests, `check()` for assertions, `group()` for organization
- K6 output: `k6 run --out json=report.json script.js` for JSON report
- Integration test profile: `@ActiveProfiles("integration")` with `application-integration.yml`
- Testcontainers reuse: `testcontainers.reuse.enable=true` in `.testcontainers.properties` for speed

## Success Metrics

- `mvn test` passes all unit tests in < 2 minutes
- Integration tests pass with real Kafka, MySQL, Redis, MongoDB
- Flash auction: 1000 bids, zero conflicts, P99 < 200ms
- Ticket rush: 5000 requests, exactly 1000 succeed, zero oversell
- Mixed workload: P99 < 500ms, error rate < 1%
