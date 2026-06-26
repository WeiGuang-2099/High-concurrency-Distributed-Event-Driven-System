# High-Concurrency Auction Platform

A real-time auction and ticketing platform built with a distributed event-driven architecture. The system handles two high-concurrency scenarios: **flash auctions** (thousands of users bidding simultaneously) and **ticket rushes** (thousands of users buying limited stock). Both scenarios are protected against overselling and data races using Redis Lua scripts, MySQL optimistic locking, and transactional compensation.

> **Load-tested (k6):** zero oversell under **200 concurrent buyers** (2,000 simultaneous reserve attempts on 100 stock → exactly 100 sold), and a Redis-Lua vs pessimistic-lock benchmark showing **~1.5x throughput** at lower tail latency. See [Performance & Load Testing](#performance--load-testing).

## Tech Stack

| Layer | Technology | Role in this project |
|-------|-----------|---------------------|
| **Language / Framework** | Java 17, Spring Boot 3.2.5 | Application runtime |
| **Microservices** | Spring Cloud 2023.0.1, Spring Cloud Alibaba 2022.0.0.0 | Service discovery, config, gateway |
| **Service Discovery / Config** | Nacos v2.3.0 | Service registry + shared configuration center |
| **API Gateway** | Spring Cloud Gateway | Routing, JWT authentication, CORS, traceId injection |
| **Message Broker (Events)** | Apache Kafka 3.6.1 (KRaft mode) | Domain event propagation (bid placed, stock reserved, order created, etc.) |
| **Message Broker (Delayed)** | RabbitMQ 3 + delayed-message-exchange plugin | Order/reservation timeout (30 min auto-cancel) |
| **Relational DB** | MySQL 8.0 + MyBatis-Plus 3.5.6 | Transactional data (users, auctions, tickets, orders) |
| **In-Memory Store** | Redis 7 | Atomic stock deduction (Lua), atomic bid validation (Lua), token blacklist, hot-auction cache |
| **Document DB** | MongoDB 7 | Event sourcing store, CQRS read-side projections |
| **Distributed Transactions** | Seata 1.8.0 (AT mode) | Cross-service transactional consistency for auction settlement |
| **Observability** | ELK 8.12, Zipkin | Log aggregation, distributed tracing |
| **Frontend** | React 18, TypeScript, Ant Design 5, React Query, Zustand | SPA with real-time WebSocket notifications |

## Architecture

```
                              ┌──────────────────────────┐
                              │    Frontend (React SPA)   │
                              └────────────┬─────────────┘
                                           │ HTTP / WebSocket
                              ┌────────────▼─────────────┐
                              │   Gateway Service :8080   │
                              │  JWT auth · CORS · TraceId│
                              └───┬────┬────┬────┬────┬───┘
                  ┌──────────────┼────┼────┼────┼────┼──────────────┐
                  │              │    │    │    │    │              │
           ┌──────▼─────┐ ┌──────▼──┐ ┌▼────┴──┐ ┌▼─────┐ ┌────────▼────────┐
           │User Service│ │Auction  │ │Ticket  │ │Order │ │Notification     │
           │   :8086    │ │Service  │ │Service │ │Svc   │ │Service :8084    │
           │register    │ │ :8081   │ │ :8082  │ │:8083 │ │WebSocket push   │
           │login(JWT)  │ │bid(Lua) │ │reserve │ │pay   │ │Kafka consumer   │
           └──────┬─────┘ └──┬──┬───┘ └──┬─────┘ └──┬───┘ └────────┬────────┘
                  │          │  │        │          │              │
                  │     ┌────┘  │        │     ┌────┘              │
                  │     ▼       ▼        ▼     ▼                   │
           ┌──────┴────────────────────────────────────┐           │
           │              Kafka Topics                  │           │
           │  auction-events · ticket-events            │           │
           │  order-events                               │           │
           └─────────────────────┬──────────────────────┘           │
                                 │                                  │
                      ┌──────────▼──────────┐                       │
                      │Event Store Service  │                       │
                      │      :8085          │                       │
                      │ MongoDB append-only │◄──────────────────────┘
                      │ + CQRS projections  │
                      └─────────────────────┘

    ┌─────────────────────────────────────────────────────────┐
    │                    Data Stores                           │
    │  MySQL 8 (5 DBs)  ·  Redis 7  ·  MongoDB 7              │
    │  RabbitMQ 3 (delayed msg)  ·  Seata Server 1.8          │
    └─────────────────────────────────────────────────────────┘
```

### Data Flow: Ticket Purchase (Oversell Prevention)

```
User → Gateway → Ticket Service
                     │
                     ├─ 1. Redis Lua script: atomically check + decrement stock
                     │     (returns immediately if insufficient)
                     │
                     ├─ 2. MySQL transaction: insert reservation + increment reserved_count
                     │     └─ If MySQL rolls back → Redis compensated via afterCompletion hook
                     │
                     ├─ 3. Publish StockReservedEvent to Kafka
                     │     └─ Event Store consumes → appends to MongoDB
                     │     └─ Notification Service consumes → WebSocket push
                     │
                     └─ 4. Send delayed message to RabbitMQ (30 min timeout)
                           └─ If not confirmed in time → auto-release stock in Redis + MySQL
```

### Data Flow: Auction Bid

```
User → Gateway → Auction Service
                     │
                     ├─ 1. Redis Lua script: atomically verify auction is ACTIVE
                     │     and bid amount > current highest, then update highest bid hash
                     │
                     ├─ 2. Publish BidPlacedEvent to Kafka
                     │     └─ If previous bidder exists → also publish BidOutbidEvent
                     │
                     └─ 3. Event Store appends to MongoDB
                         Notification Service pushes real-time updates via WebSocket

Scheduler (every 1s):
  ├─ Activate auctions whose startTime has passed (PENDING → ACTIVE)
  └─ Settle auctions whose endTime has passed (ACTIVE → SETTLED)
        ├─ If highest bid exists → Seata global TX: reserve stock + create order
        └─ If no bids → mark EXPIRED
```

## Project Structure

```
.
├── auction-common/            # Shared DTOs, event classes, exception handling, security context
├── gateway-service/           # API gateway: routing, JWT verification (RSA public key), traceId filter
├── user-service/              # User registration, login (RSA-signed JWT), profile management
├── auction-service/           # Auction CRUD, atomic bidding (Lua), lifecycle scheduler, Seata settlement
├── ticket-service/            # Stock management, atomic reserve/confirm/cancel (Lua), timeout handling
├── order-service/             # Order state machine, mock payment, CAS status transitions, timeout expiry
├── notification-service/      # Kafka event consumers, WebSocket (STOMP) push, notification persistence
├── event-store-service/       # MongoDB event sourcing, CQRS projections, snapshot + replay API
├── frontend/                  # React 18 + TypeScript SPA (Ant Design, React Query, WebSocket)
├── deploy/                    # MySQL init scripts, Nacos configs, Seata config, Logstash pipeline
└── docker-compose.yml         # All infrastructure: MySQL, Redis, MongoDB, Kafka, RabbitMQ, Nacos, Seata, ELK, Zipkin
```

## Core Technical Highlights

### 1. Atomic Stock Deduction with Redis Lua + MySQL Compensation

Ticket oversell prevention uses a two-layer approach:

- **Redis Lua script** (`reserve_ticket.lua`) performs an atomic check-and-decrement. If `stock < quantity`, it returns immediately without modifying state. This handles thousands of concurrent requests with zero oversell.
- **MySQL transaction** then creates the reservation record and increments `reserved_quantity`. If the MySQL transaction fails and rolls back, a `TransactionSynchronization.afterCompletion` hook compensates Redis by incrementing the stock back.
- The compensation runs **after** the MySQL commit/rollback decision, ensuring Redis and MySQL never diverge.

### 2. Atomic Bid Validation with Redis Lua

The `place_bid.lua` script checks auction status (`ACTIVE`) and validates the bid amount exceeds the current highest in a single atomic Redis operation. This prevents race conditions where two bids arrive simultaneously and both see the same "current highest" value.

### 3. Dual Message Broker Architecture

- **Kafka** carries all domain events (`auction-events`, `ticket-events`, `order-events`). Producers use idempotent delivery (`acks=all`, `enable.idempotence=true`), consumers use manual acknowledgment with dead-letter topics.
- **RabbitMQ** with the `rabbitmq_delayed_message_exchange` plugin handles time-delayed operations: order payment timeout (30 min) and ticket reservation expiry. When a delayed message fires, the consumer checks if the reservation/order is still pending before releasing stock.

### 4. Event Sourcing with MongoDB + CQRS Projections

The `event-store-service` maintains an append-only event log in MongoDB:

- **EventStoreConsumer** subscribes to all Kafka topics and appends every domain event with a monotonically increasing sequence number per aggregate. Idempotency is enforced via a unique index on `eventId`.
- **Projection consumers** (separate consumer groups) build materialized views: bid history, order timeline, stock movement log, auction summary.
- **Snapshot + Replay**: every 100 events per aggregate, a snapshot is stored. The `/api/admin/events/replay/{aggregateType}/{aggregateId}` endpoint reconstructs aggregate state from the latest snapshot plus subsequent events.
- **Query API** (`/api/event-store/*`) reads exclusively from projections, never touching the write-side event stream.

## Performance & Load Testing

The flash-sale (ticket reserve) path is load-tested with [k6](https://k6.io). Tests hit `ticket-service` **in isolation** on a single laptop (Java 21, Tomcat, HikariCP) with MySQL / Redis / Kafka / RabbitMQ in Docker — isolating the service under test from gateway, JWT, and other JVMs is the correct setup for a benchmark. **Full methodology, environment, and raw metrics: [loadtest/REPORT.md](loadtest/REPORT.md).**

| Scenario | Setup | Result |
|----------|-------|--------|
| **Oversell correctness** | 200 concurrent VUs · 2,000 reserve attempts on 100 stock | **Exactly 100 sold, 0 oversold, 0 server errors** · p99 683 ms · ~921 req/s |
| **Sustained throughput** | 200 VUs · 10,000 reserves on one hot stock row | **10,000 / 10,000 succeeded, 0 oversold** · ~261 req/s · p99 1.17 s |
| **Optimistic vs pessimistic** | 50 VUs · 8,000 reserves · Redis-Lua vs `SELECT … FOR UPDATE` | **Redis-Lua ~282 req/s vs pessimistic ~185 req/s (~1.5x)** · both 0 oversold |

- **Oversell is verified by a post-run invariant** (`reserved + sold ≤ total`), not assumed — and the Redis decrement count always matches MySQL's `reserved_quantity`, so the two stores never diverge under saturation.
- **The optimistic/pessimistic comparison quantifies a design tradeoff:** moving the contention check into a Redis Lua script shortens how long each request holds the MySQL row lock, yielding ~1.5x throughput over `SELECT … FOR UPDATE` at equal correctness.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Java 17 (for building services)
- Node.js 18+ (for frontend development)
- ~10 GB free RAM for all containers

### Start Infrastructure

```bash
docker-compose up -d
```

This starts 12 containers: MySQL, Redis, MongoDB, Kafka (KRaft), RabbitMQ, Nacos, Seata Server, Zipkin, Elasticsearch, Logstash, Kibana. Wait ~2 minutes for all health checks to pass.

### Build and Run Services

```bash
# Build all services
mvn clean package -DskipTests

# Run each service (in separate terminals)
java -jar user-service/target/user-service-1.0.0-SNAPSHOT.jar
java -jar auction-service/target/auction-service-1.0.0-SNAPSHOT.jar
java -jar ticket-service/target/ticket-service-1.0.0-SNAPSHOT.jar
java -jar order-service/target/order-service-1.0.0-SNAPSHOT.jar
java -jar notification-service/target/notification-service-1.0.0-SNAPSHOT.jar
java -jar event-store-service/target/event-store-service-1.0.0-SNAPSHOT.jar
java -jar gateway-service/target/gateway-service-1.0.0-SNAPSHOT.jar
```

### Run Frontend

```bash
cd frontend
npm install
npm run dev
```

### Access Points

| Service | URL |
|---------|-----|
| Frontend | http://localhost:5173 |
| API Gateway | http://localhost:8080 |
| Nacos Console | http://localhost:8848/nacos |
| Seata Console | http://localhost:7091 |
| Zipkin UI | http://localhost:9411 |
| Kibana | http://localhost:5601 |
| RabbitMQ Management | http://localhost:15672 (guest/guest) |

### Service Port Allocation

| Service | Port |
|---------|------|
| gateway-service | 8080 |
| auction-service | 8081 |
| ticket-service | 8082 |
| order-service | 8083 |
| notification-service | 8084 |
| event-store-service | 8085 |
| user-service | 8086 |

## API Overview

All API responses use a unified envelope:

```json
{
  "code": 200,
  "message": "success",
  "data": { ... },
  "timestamp": "2026-05-21T10:30:00Z",
  "traceId": "abc123"
}
```

### User Service (`/api/users`)

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| POST | `/register` | Register a new user | Public |
| POST | `/login` | Login, returns RSA-signed JWT | Public |
| POST | `/logout` | Logout, adds token JTI to Redis blacklist | Required |
| GET | `/me` | Get current user profile | Required |
| PUT | `/me` | Update profile (email) | Required |

### Auction Service (`/api/auctions`)

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| GET | `/` | List auctions (paginated) | Required |
| GET | `/hot` | Get hot auctions (Redis cached) | Required |
| GET | `/{id}` | Get auction details | Required |
| POST | `/{auctionId}/bids` | Place a bid (atomic Lua validation) | Required |
| GET | `/{auctionId}/bids` | Bid history (username masked) | Required |
| POST | `/admin/auctions` | Create auction | Admin |

### Ticket Service (`/api/tickets`)

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| GET | `/events/{eventId}` | Get stock for an event | Required |
| POST | `/reserve` | Reserve tickets (atomic Lua deduction) | Required |
| POST | `/{reservationId}/confirm` | Confirm reservation | Required |
| DELETE | `/{reservationId}` | Cancel reservation | Required |
| POST | `/admin/tickets` | Create ticket stock | Admin |

### Order Service (`/api/orders`)

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| POST | `/` | Create order from ticket reservation | Required |
| GET | `/{id}` | Get order details | Required |
| GET | `/` | List user's orders | Required |
| POST | `/{id}/pay` | Pay order (mock payment, 95% success rate) | Required |
| POST | `/{id}/cancel` | Cancel order | Required |

### Event Store Service (`/api/event-store`)

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| GET | `/auctions/{id}/history` | Bid history projection | Required |
| GET | `/orders/{id}/timeline` | Order status timeline | Required |
| GET | `/stock/{eventId}/movements` | Stock movement log | Required |
| POST | `/admin/events/replay/{aggregateType}/{aggregateId}` | Replay aggregate state | Admin |

### Notification Service (`/api/notifications`)

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| GET | `/` | List notifications | Required |
| PUT | `/{id}/read` | Mark notification as read | Required |

Real-time notifications are pushed via WebSocket (STOMP) at `ws://localhost:8084/ws`.

## Testing

### Unit Tests

86 unit tests across 4 core services, all passing:

| Service | Test File | Tests | Coverage |
|---------|-----------|-------|----------|
| user-service | `UserServiceImplTest` | 15 | Register, login (success/invalid credentials), profile, update, logout, token blacklist |
| auction-service | `BidServiceImplTest` | 7 | Bid acceptance/rejection (Lua), outbid event publishing, same-user logic |
| auction-service | `AuctionServiceImplTest` | 8 | Auction creation, validation, listing, detail retrieval |
| auction-service | `BidHistoryServiceImplTest` | 8 | Username masking (6 length cases), bid history query |
| ticket-service | `TicketStockServiceImplTest` | 19 | Reserve (success/out-of-stock/not-found/not-ready), confirm, cancel, settle, stock calculation |
| order-service | `OrderServiceImplTest` | 29 | Create, pay (success/failure/idempotent/CAS), cancel, expire, state machine transitions |

```bash
mvn test
```

### Load Tests (k6)

The ticket-rush flash-sale path is benchmarked end-to-end with k6 — **zero oversell under 200 concurrent VUs**, plus an optimistic-vs-pessimistic throughput comparison. Headline numbers are in [Performance & Load Testing](#performance--load-testing); full report in [loadtest/REPORT.md](loadtest/REPORT.md).

```bash
# oversell proof: 2,000 concurrent reserve attempts on 100 stock
k6 run -e STOCK=100 -e ITERATIONS=2000 -e VUS=200 loadtest/ticket-rush.js
```

### Not Yet Implemented

- Integration tests with Testcontainers (Kafka E2E, Seata TX commit/rollback, Redis Lua concurrency)
- k6 load test for the auction-bid path (the ticket-reserve path is done — see above)

## Current Status

| Phase | Description | Status |
|-------|-------------|--------|
| P0 | Project skeleton, Docker Compose, MySQL init, Nacos, Gateway | Done |
| P1 | User service (registration, JWT auth) | Done |
| P2 | Auction service (CRUD, atomic bidding, lifecycle scheduler) | Done |
| P3 | Ticket service (Lua stock deduction, reserve/confirm/cancel) | Done |
| P4 | Order service (state machine, payment, Seata distributed TX) | Done |
| P5 | Event sourcing (MongoDB append-only, CQRS projections, replay) | Done |
| P6 | Frontend (React SPA, WebSocket notifications) | Done |
| P7 | Observability (structured logging, Sentinel rate limiting, Kibana dashboards) | Infrastructure containers running; service-side logback/Sentinel config not yet implemented |
| P8 | Testing (unit, integration, K6 load tests) | 86 unit tests + k6 ticket-rush load tests done (zero oversell verified, optimistic-vs-pessimistic benchmarked); integration tests and auction-bid load test pending |
| P9 | README, architecture diagrams, demo guide | This README; diagrams and demo guide pending |

## Default Credentials

All credentials below are demo defaults for local development only:

| Service | Username | Password |
|---------|----------|----------|
| MySQL | root | root |
| MongoDB | root | root |
| RabbitMQ | guest | guest |
| Nacos | (auth disabled) | - |
| Seata Console | seata | seata |
