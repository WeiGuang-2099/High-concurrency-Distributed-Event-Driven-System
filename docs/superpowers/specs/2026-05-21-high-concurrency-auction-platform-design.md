# High-Concurrency Distributed Event-Driven Auction Platform

## Project Overview

A real-time auction platform with ticketing as a secondary scenario (concert ticket auctions), designed to demonstrate expertise in high-concurrency systems for FinTech and e-commerce job applications in the Australian market (Sydney/Melbourne).

**Core technical highlights**:
1. Oversell prevention with Redis Lua atomic stock deduction
2. Distributed transaction consistency with Seata AT + Kafka transactional messages
3. Traffic shaping with Sentinel rate limiting + Kafka peak shaving
4. Event Sourcing + CQRS with MongoDB event store

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Java 17, Spring Boot 3.x, Spring Cloud Alibaba |
| API Gateway | Spring Cloud Gateway + Sentinel |
| Service Registry & Config | Nacos (discovery + config center) |
| Service-to-Service | OpenFeign (sync) + Kafka (async) |
| Message Queue | Kafka (core transactions) + RabbitMQ (notifications + delayed messages) |
| Primary DB | MySQL 8.0 (Database per Service) |
| Cache | Redis 7.x (cache, distributed lock, stock pre-deduction) |
| Event Store | MongoDB 7.x (event log, auction history) |
| Distributed TX | Seata AT mode (Seata Server as TC) |
| Frontend | React + TypeScript + Ant Design Pro |
| Real-time | WebSocket (STOMP over SockJS) |
| Deployment | Docker Compose |
| Observability | ELK Stack (Elasticsearch + Logstash + Kibana) |
| Tracing | Micrometer Tracing + Brave + Zipkin |
| Load Testing | K6 (scriptable, JS-based, Docker-friendly) |

## System Architecture

```
                    [React Frontend]
                         |
                  [API Gateway :8080]
                /   /    |     \     \
               /   /     |      \     \
    user-  auction- ticket- order-  notification-
    service service  service  service  service
       \     |  \      /       /       /
        \    |   \    /       /       /
         \   |    [Kafka Cluster]    /
          \  |        |   \         /
           \ |        |    \       /
           event-  MongoDB  RabbitMQ
           store   (events) (notify+delay)
            |   \     |        |
          Redis  MySQL(per-svc) WebSocket
     (cache+lock)                  push
```

**Service Registry & Config**: Nacos (8848) -- service discovery + shared configuration

**Inter-service Communication**:
- **Async**: Kafka for event-driven flows (all state changes)
- **Sync**: OpenFeign for distributed transaction coordination (Seata 2PC requires synchronous calls)
- **Feign clients**: AuctionFeignClient, TicketFeignClient, OrderFeignClient

## Microservice Breakdown

### user-service (Port 8086)

- User registration with BCrypt password hashing
- JWT token issuance and validation (RS256 asymmetric key)
- User profile management
- Token refresh and logout (Redis token blacklist)
- Shares public key with gateway-service for JWT verification

### gateway-service (Port 8080)

- API routing and forwarding
- Rate limiting with Sentinel (QPS limit, IP hotspot limit)
- JWT authentication via user-service public key (no need to call user-service per request)
- Request/response logging with traceId injection
- Global CORS configuration

### auction-service (Port 8081)

- Auction lifecycle: create, bid, settle, expire
- Real-time bid validation with Redis Lua (atomic check: bid > current highest)
- Publish auction events to Kafka topic `auction-events`
- Auction expiration: scheduled via Spring `@Scheduled` + DB polling (every 1 second), marks expired auctions and triggers settlement
- Bid tie-breaking: same-price bids resolved by timestamp (first-come-first-served, Redis Lua returns failure for bid <= current)
- Settles winning bids via OpenFeign call to ticket-service and order-service (Seata 2PC)

### ticket-service (Port 8082)

- Ticket stock management per event and ticket type
- Oversell prevention: Redis Lua atomic pre-deduction
- Stock reservation with 30-minute timeout (timeout message sent to RabbitMQ delayed queue)
- Stock release on order cancellation/expiration
- Publish stock events to Kafka topic `ticket-events`
- Internal endpoint `POST /api/tickets/internal/settle-reserve` for Seata 2PC auction settlement (MySQL-only, no Redis, so Seata undo_log has full rollback control)
- Oversell prevention: Redis Lua atomic pre-deduction
- Stock reservation with 30-minute timeout (timeout message sent to RabbitMQ delayed queue)
- Stock release on order cancellation/expiration
- Publish stock events to Kafka topic `ticket-events`

### order-service (Port 8083)

- Order creation from auction settlement or ticket purchase
- Payment flow state machine (Created -> Paying -> Paid -> Completed)
- Mock payment service: `PaymentMockController` simulates payment gateway with configurable success rate and latency
- Distributed transaction coordination with Seata AT for critical paths
- Kafka transactional messages for non-critical paths
- Timeout handling: order-service listens to RabbitMQ delayed queue for stock rollback
- Publish order events to Kafka topic `order-events`

### notification-service (Port 8084)

- Consumes events from all Kafka topics
- Real-time WebSocket push (STOMP over SockJS) for bid updates
- WebSocket authentication: validates JWT token on CONNECT frame, rejects unauthenticated connections
- Email notifications via RabbitMQ (email.direct exchange)
- SMS notifications via RabbitMQ (sms.topic exchange)
- In-app notification aggregation

### event-store-service (Port 8085)

- Append-only event storage in MongoDB
- Idempotent consumer: uses MongoDB unique index on `(aggregateId, sequenceNumber)` to deduplicate events
- Event replay for state reconstruction
- CQRS read side: maintains projections in Redis and MongoDB
- Snapshot generation every 100 events per aggregate
- Supports queries: auction history, order timeline, stock movements
- Other services interact via REST API (not direct MongoDB connection)

## Kafka Topic Design

| Topic | Partitions | Purpose | Producer | Consumers |
|-------|-----------|---------|----------|-----------|
| `auction-events` | 6 | Auction lifecycle events | auction-service | event-store, notification |
| `ticket-events` | 3 | Stock reservation/release events | ticket-service | event-store, order |
| `order-events` | 3 | Order state changes | order-service | event-store, notification |
| `dead-letter` | 1 | Failed message dead letter queue | all services | alert service |

## RabbitMQ Exchange & Queue Design

| Exchange | Type | Queue | Purpose |
|----------|------|-------|---------|
| `notification.fanout` | fanout | ws-push, in-app | Real-time WebSocket and in-app notifications |
| `email.direct` | direct | email-queue | Email notifications (routing key: email type) |
| `sms.topic` | topic | sms-queue | SMS notifications (routing key: sms.*.urgent) |
| `delay.exchange` | x-delayed-message | stock-release-queue | Delayed stock release on order timeout (RabbitMQ delayed message plugin) |

**Why RabbitMQ for delayed messages**: Kafka does not natively support delayed/timed messages. RabbitMQ's `rabbitmq_delayed_message_exchange` plugin provides clean delayed delivery. Used for:
- 30-minute stock reservation timeout -> rollback
- Auction countdown expiration trigger (backup mechanism)

## Event Types

### Auction Events
```
AuctionCreated, BidPlaced, BidOutbid, AuctionSettled, AuctionExpired
```

### Ticket Events
```
TicketCreated, StockReserved, StockReleased, StockConfirmed
```

### Order Events
```
OrderCreated, PaymentInitiated, PaymentCompleted, OrderCancelled, OrderExpired
```

## Core Data Flows

### Auction Bid Flow

```
User bids -> Gateway (JWT auth + rate limit) -> auction-service
  -> Redis Lua atomic check (bid > current highest, reject bid <= current)
    -> Success: publish BidPlacedEvent to Kafka
      -> event-store: idempotent append to MongoDB (aggregateId + seqNo unique constraint)
      -> notification-service: push via WebSocket
      -> ticket-service: update stock reservation
    -> Failure: return "Bid too low"
```

### Ticket Purchase Flow (Oversell Prevention)

```
User buys ticket -> Gateway -> ticket-service
  -> Redis Lua EVAL: GET stock key -> if >= 1 then DECR, return success
    -> Stock >= 0:
      -> Publish StockReservedEvent to Kafka
      -> Send delayed message to RabbitMQ (30-min TTL) for stock release
      -> order-service consumes and creates order
      -> 30-min timeout (no payment) -> RabbitMQ delayed message delivered
        -> ticket-service receives -> rollback stock in Redis + MySQL
    -> Stock < 0:
      -> Redis INCR rollback
      -> Return "Out of stock"
```

### Distributed Transaction Flow

```
Auction settles -> auction-service (Seata TM, via @GlobalTransactional)
  -> OpenFeign call: ticket-service /api/tickets/internal/settle-reserve (Seata RM, MySQL-only)
  -> OpenFeign call: order-service /api/orders/internal/auction (Seata RM)
  -> All succeed: Seata TC commits; async Kafka consumer syncs Redis stock from MySQL
  -> Any fails: Seata TC rolls back both MySQL operations via undo_log (Redis never touched)
```

### User Auth Flow

```
Register/Login -> user-service
  -> Validate credentials -> issue JWT (RS256, private key signs)
  -> Gateway validates JWT with public key (no user-service call needed)
  -> JWT payload: userId, roles, exp
  -> Redis blacklist on logout: `token:blacklist:{jti}` with TTL = remaining expiry
```

## Technical Highlights Detail

### 1. Oversell Prevention

- Redis Lua script executes GET + conditional DECR atomically within Redis single thread
- Stock key format: `stock:{eventId}:{ticketType}`
- Async sync to MySQL via Kafka for eventual consistency
- Avoids distributed lock contention between Redis and MySQL
- **Redis recovery**: On Redis restart, stock data is rebuilt from MySQL `ticket_stock` table via `ticket-service` startup listener. During rebuild, ticket purchase API returns 503 (circuit breaker)

### 2. Distributed Transaction Consistency

- **Critical path** (bid -> deduction -> order): Seata AT mode, 2PC strong consistency. Requires Seata Server (TC) running as standalone service. Uses `settle-reserve` (MySQL-only, no Redis) so Seata undo_log has full rollback control.
- **Non-critical path** (notification, logging, stats): Kafka transactional messages + idempotent consumption, eventual consistency
- **Compensation**: Order timeout triggers RabbitMQ delayed message for stock rollback
- **Seata Server**: Deployed as standalone container in Docker Compose, uses MySQL (`seata_db`) for transaction log storage

### 3. Traffic Shaping

**Three-layer defense**:
1. Gateway layer: Spring Cloud Gateway + Sentinel, QPS limiting + IP hotspot limiting
2. Service layer: Semaphore controlling concurrent bid threads, preventing DB overload
3. Cache layer: Redis pre-deduction, async DB write via Kafka consumer batch processing

**Peak shaving**: Bid requests enter Kafka via producer, consumer processes at DB-sustainable rate. Frontend gets real-time feedback via WebSocket ("Your bid has been submitted") without synchronous DB wait.

### 4. Event Sourcing + CQRS

**Write side (Command)**:
- All write requests produce events via Kafka Producer
- event-store-service appends events to MongoDB (aggregateId + sequenceNumber ordered)
- Idempotent write: MongoDB unique compound index on `(aggregateId, sequenceNumber)` prevents duplicate events
- Supports event replay for state reconstruction

**Read side (Query)**:
- Event projections update Redis cache and MongoDB query views in real-time
- Example: `AuctionSummaryProjection` listens to all auction events, maintains "current highest bid" materialized view
- Frontend queries hit Redis cache; cache miss falls back to MongoDB
- **Cache consistency**: Write-side Kafka consumer invalidates related Redis keys on each event. Example: `auction:current:{auctionId}` is deleted after BidPlacedEvent, forcing next read to rebuild from MongoDB projection

**Snapshot**: Snapshot every 100 events per aggregate to accelerate replay.

### 5. Idempotent Consumer Design

Every Kafka consumer implements idempotency:
- **Event store**: MongoDB unique constraint `(aggregateId, sequenceNumber)` rejects duplicates
- **Order service**: Checks `order` table by `idempotencyKey` (derived from eventId + eventType) before processing
- **Notification service**: Maintains `notification_log` table with `eventId` unique constraint
- **General pattern**: Each consumer checks if event has been processed before executing business logic

### 6. Auction Expiration & Settlement

- **Primary mechanism**: `auction-service` runs `@Scheduled(fixedRate = 1000)` to poll MySQL for auctions where `end_time <= NOW()` and `status = ACTIVE`
- **Backup mechanism**: RabbitMQ delayed message sent when auction is created (TTL = auction duration) as fallback trigger
- **Settlement logic**: Find highest bid -> mark auction as SETTLED -> trigger Seata TX for stock reservation + order creation
- **No bids scenario**: Mark auction as EXPIRED, no order created

## Unified Error Response Format

All services return errors in this JSON structure:
```json
{
  "code": 400,
  "message": "Bid too low",
  "timestamp": "2026-05-21T10:30:00Z",
  "traceId": "abc123"
}
```
Gateway transforms downstream errors into this format. Frontend Axios interceptor reads this format for error display.

## Core REST API Contract

### Auction APIs
```
POST   /api/auctions                    # Create auction (admin)
GET    /api/auctions                    # List auctions (paginated)
GET    /api/auctions/{id}               # Get auction detail
POST   /api/auctions/{id}/bids          # Place bid
GET    /api/auctions/{id}/bids          # Get bid history
GET    /api/auctions/hot                # Get hot auctions (cached)
```

### Ticket APIs
```
GET    /api/tickets/events/{eventId}    # Get ticket stock for event
POST   /api/tickets/reserve             # Reserve tickets (returns reservationId)
POST   /api/tickets/{reservationId}/confirm  # Confirm reservation after payment
DELETE /api/tickets/{reservationId}     # Cancel reservation
```

### Order APIs
```
POST   /api/orders                      # Create order
GET    /api/orders/{id}                 # Get order detail
GET    /api/orders?userId={userId}      # List user orders
POST   /api/orders/{id}/pay             # Initiate payment
POST   /api/orders/{id}/cancel          # Cancel order
```

### User APIs
```
POST   /api/users/register              # Register
POST   /api/users/login                 # Login (returns JWT)
POST   /api/users/logout                # Logout (blacklist JWT)
GET    /api/users/me                    # Get current user profile
PUT    /api/users/me                    # Update profile
```

### Notification APIs
```
GET    /api/notifications               # List my notifications (paginated)
PUT    /api/notifications/{id}/read     # Mark as read
WS     /ws/notifications                # WebSocket endpoint (STOMP)
```

### Admin APIs
```
POST   /api/admin/auctions              # Create auction
PUT    /api/admin/auctions/{id}         # Update auction
POST   /api/admin/tickets               # Create ticket batch
GET    /api/admin/stats                 # Dashboard statistics
```

## Frontend Design

### Pages
- Home: Hot auctions, upcoming events, countdown timers
- Auction Detail: Real-time bidding, current price, bid history, WebSocket updates
- Ticket Purchase: Live stock count, seat/ticket type selection, order placement
- User Center: My bids, my orders, notifications
- Login/Register: JWT-based auth flow
- Admin Panel: Create auctions/tickets, stock management, analytics dashboard

### Key Technologies
- WebSocket (STOMP over SockJS) for real-time bid updates and notifications
- React Query for server state management
- Zustand for client state (current user, auction state)
- Ant Design Pro for UI components

### WebSocket Authentication
- Client sends JWT in STOMP CONNECT frame headers (`Authorization: Bearer <token>`)
- Server-side `StompChannelInterceptor` validates token before accepting connection
- Invalid/expired token -> connection rejected with error frame

## Testing Strategy

### Unit Tests
- JUnit 5 + Mockito for service layer
- Target: 70%+ line coverage on business logic
- Focus: bid validation logic, stock deduction Lua script, order state machine transitions

### Integration Tests
- Testcontainers for Kafka, MySQL, Redis, MongoDB
- Each service has integration test profile (`application-integration.yml`)
- Verify: Kafka produce/consume round-trip, Seata TX commit/rollback, Redis Lua script execution

### Contract Tests
- Spring Cloud Contract for OpenFeign client verification
- Ensures API compatibility between services

### Load Testing (K6)
- **Scenario 1 - Flash auction**: 1000 concurrent bidders on single auction, verify no oversell
- **Scenario 2 - Ticket rush**: 5000 concurrent ticket purchases for 1000 available tickets
- **Scenario 3 - Mixed workload**: Simultaneous bidding + ticket purchasing + notifications
- Metrics collected: P50/P95/P99 latency, error rate, Kafka consumer lag
- K6 scripts stored in `deploy/k6/`

## Deployment (Docker Compose)

### Services (~17 containers)

**Microservices (7)**: gateway, user, auction, ticket, order, notification, event-store

**Infrastructure (8)**:
- mysql (1 instance, multiple databases via init scripts)
- redis
- mongodb
- kafka (1 broker, KRaft mode -- no ZooKeeper needed)
- rabbitmq (with delayed message plugin)
- nacos (standalone)
- seata-server
- zipkin

**Observability (3)**: elasticsearch, logstash, kibana

### KRaft Mode (No ZooKeeper)
Kafka 3.3+ supports KRaft mode, eliminating ZooKeeper dependency. This reduces container count by 3 and memory usage by ~2GB.

### Resource Requirements
- Docker memory allocation: 10GB+
- Kafka broker heap: 1GB
- Elasticsearch heap: 1.5GB
- Seata Server heap: 512MB

### ELK Integration
- Each microservice outputs JSON logs via Logback (`logstash-logback-encoder`)
- Logback pattern includes: traceId, spanId, service name, level, timestamp
- Logstash collects and parses with unified fields
- Kibana preset dashboards: error rate, P99 latency, Kafka consumer lag, auction QPS

### Distributed Tracing
- Micrometer Tracing + Brave bridge (not Sleuth -- Sleuth is deprecated in Spring Boot 3.x)
- Zipkin receives trace spans from all services
- traceId propagated through: HTTP headers (gateway -> service), Kafka headers (producer -> consumer), RabbitMQ headers
- Zipkin UI accessible at port 9411

### Health Checks & Startup Order
```yaml
# Startup dependency order
1. mysql, redis, mongodb          # Data stores first
2. kafka, rabbitmq                # Message brokers
3. nacos, seata-server, zipkin    # Infrastructure
4. elasticsearch                  # Log storage
5. user-service                   # Auth dependency
6. gateway-service                # API entry (depends on user-service for JWT pubkey)
7. auction, ticket, order         # Core services
8. notification, event-store      # Event processing
9. logstash, kibana               # Observability (can start last)
```
Each service has `/actuator/health` endpoint. Docker Compose `depends_on` with `condition: service_healthy`.

## Nacos Configuration Management

- Shared config: `shared-db.yml`, `shared-redis.yml`, `shared-kafka.yml`
- Per-service config: `{service-name}.yml`
- Dynamic refresh: `@RefreshScope` for rate limit thresholds, feature flags
- Config stored in Nacos, not in local application.yml (local only has Nacos address)

## Database Schema (Key Tables)

### user-service (MySQL: user_db)
- `user`: id, username, email, password_hash, role (USER/ADMIN), created_at, updated_at

### auction-service (MySQL: auction_db)
- `auction`: id, event_name, starting_price, current_highest_bid, status, start_time, end_time, winner_id
- `bid`: id, auction_id, user_id, amount, created_at
- Index: `idx_auction_status_end_time` on (status, end_time) for expiration polling

### ticket-service (MySQL: ticket_db)
- `ticket_stock`: id, event_id, ticket_type, total_quantity, reserved_quantity, sold_quantity, version (optimistic lock)
- `reservation`: id, stock_id, user_id, quantity, status, expire_at

### order-service (MySQL: order_db)
- `orders`: id, user_id, type (AUCTION/TICKET), reference_id, amount, status, created_at, paid_at
- `payment`: id, order_id, payment_method, amount, status
- `idempotency_log`: idempotency_key (unique), event_type, processed_at -- for idempotent Kafka consumption

### seata (MySQL: seata_db)
- `branch_table`, `global_table`, `lock_table` -- Seata Server transaction coordination tables

### event-store (MongoDB)
- `events`: aggregateId, aggregateType, eventType, sequenceNumber, payload, metadata, timestamp
  - Unique compound index: `(aggregateId, sequenceNumber)`
- `snapshots`: aggregateId, aggregateType, sequenceNumber, state, timestamp

## Phased Development Plan

| Phase | Content | Duration |
|-------|---------|----------|
| P0 | Project skeleton: Maven multi-module structure + Docker Compose (all infra) + Nacos + Gateway + MySQL schemas + Seata Server | 1 week |
| P1 | User service: registration + JWT + Gateway auth integration | 2-3 days |
| P2 | Core auction: auction-service + Redis bid Lua + Kafka events + WebSocket push + scheduled expiration | 1-2 weeks |
| P3 | Ticket stock: ticket-service + Lua oversell prevention + pre-deduction + RabbitMQ delayed queue | 1 week |
| P4 | Order & TX: order-service + mock payment + Seata distributed TX + timeout rollback | 1-2 weeks |
| P5 | Event sourcing: event-store + CQRS read/write split + snapshots + cache invalidation | 1 week |
| P6 | Frontend: React UI + real-time push + admin panel | 2 weeks |
| P7 | Observability: ELK + Zipkin tracing + Sentinel rate limiting | 1 week |
| P8 | Testing: unit + integration (Testcontainers) + K6 load tests + generate report | 1 week |
| P9 | Wrap-up: README + architecture diagrams + interview prep docs | 3 days |

## Project Structure

```
high-concurrency-auction-platform/
├── pom.xml                          # Parent POM (multi-module)
├── docker-compose.yml
├── gateway-service/
│   ├── src/main/java/com/auction/gateway/
│   │   ├── config/                  # Sentinel rules, CORS, JWT filter
│   │   ├── filter/                  # Auth filter, logging filter
│   │   └── handler/                 # Rate limit handler, fallback
│   └── Dockerfile
├── user-service/
│   ├── src/main/java/com/auction/user/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── domain/
│   │   ├── repository/
│   │   ├── security/                # JWT provider, BCrypt config
│   │   └── config/
│   └── Dockerfile
├── auction-service/
│   ├── src/main/java/com/auction/auction/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── domain/
│   │   ├── repository/
│   │   ├── event/                   # Kafka producer/consumer
│   │   ├── client/                  # OpenFeign clients
│   │   ├── scheduler/               # Auction expiration polling
│   │   └── config/
│   └── Dockerfile
├── ticket-service/
│   ├── src/main/java/com/auction/ticket/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── domain/
│   │   ├── repository/
│   │   ├── event/
│   │   ├── lua/                     # Redis Lua scripts
│   │   └── config/
│   └── Dockerfile
├── order-service/
│   ├── src/main/java/com/auction/order/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── domain/
│   │   ├── repository/
│   │   ├── event/
│   │   ├── statemachine/            # Order/Payment state machine
│   │   ├── client/                  # OpenFeign clients
│   │   ├── payment/                 # Mock payment provider
│   │   └── config/
│   └── Dockerfile
├── notification-service/
│   ├── src/main/java/com/auction/notification/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── websocket/               # STOMP config, auth interceptor
│   │   ├── event/
│   │   └── config/
│   └── Dockerfile
├── event-store-service/
│   ├── src/main/java/com/auction/eventstore/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── projection/              # CQRS projections
│   │   ├── snapshot/
│   │   ├── domain/
│   │   ├── event/
│   │   └── config/
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── pages/
│   │   ├── components/
│   │   ├── hooks/                   # WebSocket hook, auth hook
│   │   ├── stores/                  # Zustand stores
│   │   ├── api/                     # React Query + Axios
│   │   └── utils/
│   ├── package.json
│   └── Dockerfile
├── deploy/
│   ├── mysql/init/                  # Schema init scripts (one per database)
│   ├── redis/lua/                   # Lua scripts for stock deduction
│   ├── rabbitmq/                    # Exchange/queue definitions
│   ├── logstash/pipeline/           # Logstash config
│   ├── kibana/dashboards/           # Saved dashboards
│   ├── k6/                          # Load test scripts
│   ├── zipkin/                      # Zipkin config
│   └── seata/                       # Seata Server config
├── docs/
│   ├── architecture/
│   ├── api/
│   └── interview-prep/
└── shared/                          # Shared library (optional)
    └── auction-common/              # Common DTOs, Kafka event classes, utilities
```
