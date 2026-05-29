# P3: Ticket Service Design

## Scope

Core ticket stock management with Redis Lua oversell prevention, MySQL persistence, Kafka event broadcasting, and RabbitMQ delayed queue for 30-minute reservation timeout. Seata distributed transaction integration is deferred to P4.

## API Endpoints

### Public (JWT required)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tickets/events/{eventId}` | List all ticket types and stock for an event |
| POST | `/api/tickets/reserve` | Reserve tickets (atomic Redis deduction) |
| POST | `/api/tickets/{reservationId}/confirm` | Confirm reservation after payment |
| DELETE | `/api/tickets/{reservationId}` | Cancel reservation |

### Admin (ADMIN role)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/admin/tickets` | Create ticket batch (MySQL + Redis warmup) |

### DTOs

- `ReserveRequest`: eventId (Long), ticketType (String), quantity (int)
- `ReserveResponse`: reservationId (Long), eventId (Long), ticketType (String), quantity (int), expireAt (LocalDateTime)
- `TicketStockResponse`: stockId, eventId, ticketType, totalQuantity, availableQuantity, reservedQuantity, soldQuantity
- `CreateTicketRequest`: eventId (Long), ticketType (String), totalQuantity (int)

## Redis Lua Scripts

### reserve_ticket.lua

Keys: `stock:{eventId}:{ticketType}`
Args: quantity

Logic: GET key -> if nil return {-1, "STOCK_NOT_FOUND"} -> if < quantity return {-1, "OUT_OF_STOCK"} -> DECRBY by quantity -> return {1, remaining}

### release_ticket.lua

Keys: `stock:{eventId}:{ticketType}`
Args: quantity

Logic: INCRBY by quantity -> return {1, "OK"}

### Key Format

`stock:{eventId}:{ticketType}` stores available count (total - reserved - sold).

## Kafka Events

### New event classes (auction-common)

- `StockReservedEvent`: eventId, ticketType, reservationId, userId, quantity
- `StockConfirmedEvent`: reservationId, userId
- `StockReleasedEvent`: reservationId, eventId, ticketType, quantity, reason (TIMEOUT/CANCELLED)

### Topic configuration

- Topic: `ticket-events` (3 partitions, 1 replica)
- DLT: `ticket-events-dlt`
- Error handler: retry 3x with 1s gap, then DLT

## RabbitMQ Delayed Queue

### Exchange

- Name: `delay.exchange`
- Type: `x-delayed-message`
- Argument: `x-delayed-type=direct`

### Queue

- Name: `stock-release-queue`
- Binding: routing key `stock.release`

### Flow

1. On successful reserve, send message to `delay.exchange` with header `x-delay=1800000` (30 min), routing key `stock.release`, payload: reservationId
2. Consumer on `stock-release-queue` checks reservation status in MySQL
3. If PENDING: rollback Redis via release_ticket.lua, update MySQL (reservation->EXPIRED, ticket_stock.reserved_quantity -= quantity), publish StockReleasedEvent to Kafka
4. If CONFIRMED/CANCELLED: ignore (idempotent)

## Startup Redis Warmup

`ApplicationRunner` bean loads all `ticket_stock` rows from MySQL, computes `available = total - reserved - sold`, sets Redis keys. An `AtomicBoolean ready` flag gates the reserve endpoint -- returns HTTP 503 if not ready.

## Reserve Flow

```
POST /api/tickets/reserve
  -> UserContextFilter extracts userId from gateway headers
  -> TicketStockServiceImpl.reserve(userId, eventId, ticketType, quantity)
     -> Redis Lua: reserve_ticket.lua(stock:{eventId}:{ticketType}, quantity)
     -> FAIL: throw BusinessException(400, "Out of stock")
     -> OK:
        -> MySQL: INSERT reservation (PENDING, expire_at=now+30min)
                 UPDATE ticket_stock SET reserved_quantity += quantity WHERE id=?
        -> Kafka: send StockReservedEvent to ticket-events
        -> RabbitMQ: send delayed message (30min, reservationId)
        -> return ReserveResponse
```

## Timeout Rollback Flow

```
RabbitMQ delayed message delivered (30min later)
  -> StockReleaseConsumer.handleStockRelease(reservationId)
     -> MySQL: SELECT reservation WHERE id=? AND status='PENDING'
     -> If not PENDING: skip (idempotent)
     -> If PENDING:
        -> Redis Lua: release_ticket.lua(stock:{eventId}:{ticketType}, quantity)
        -> MySQL: UPDATE reservation SET status='EXPIRED'
                 UPDATE ticket_stock SET reserved_quantity -= quantity
        -> Kafka: send StockReleasedEvent(reason="TIMEOUT")
```

## Confirm Flow

```
POST /api/tickets/{reservationId}/confirm
  -> TicketStockServiceImpl.confirm(userId, reservationId)
     -> MySQL: SELECT reservation WHERE id=? AND user_id=?
     -> If not PENDING: throw BusinessException
     -> MySQL: UPDATE reservation SET status='CONFIRMED'
              UPDATE ticket_stock SET reserved_quantity -= quantity, sold_quantity += quantity
     -> Kafka: send StockConfirmedEvent
     -> return success
```

## Cancel Flow

```
DELETE /api/tickets/{reservationId}
  -> TicketStockServiceImpl.cancel(userId, reservationId)
     -> MySQL: SELECT reservation WHERE id=? AND user_id=?
     -> If not PENDING: throw BusinessException
     -> Redis Lua: release_ticket.lua(stock:{eventId}:{ticketType}, quantity)
     -> MySQL: UPDATE reservation SET status='CANCELLED'
              UPDATE ticket_stock SET reserved_quantity -= quantity
     -> Kafka: send StockReleasedEvent(reason="CANCELLED")
     -> return success
```

## Dependencies to Add (ticket-service pom.xml)

- `spring-boot-starter-data-redis`
- `spring-boot-starter-data-redis-reactive` (for Lettuce connection factory)
- `spring-kafka`
- `spring-boot-starter-amqp` (RabbitMQ)
- `mybatis-plus-spring-boot3-starter`
- `mysql-connector-j`

## File Structure

```
ticket-service/src/main/java/com/auction/ticket/
  controller/
    TicketController.java
    AdminTicketController.java
    dto/ (ReserveRequest, ReserveResponse, TicketStockResponse, CreateTicketRequest)
  domain/
    entity/ (TicketStock, Reservation)
    enums/ (ReservationStatus)
  repository/ (TicketStockMapper, ReservationMapper)
  service/
    TicketStockService.java
    impl/TicketStockServiceImpl.java
  event/TicketEventProducer.java
  consumer/StockReleaseConsumer.java
  config/ (RedisConfig, KafkaConfig, RabbitMQConfig, MyBatisPlusConfig)
  security/ (UserContextFilter, UserContextHolder)
  exception/ (BusinessException, GlobalExceptionHandler)
  startup/RedisStockWarmer.java

ticket-service/src/main/resources/
  lua/ (reserve_ticket.lua, release_ticket.lua)
  application.yml (updated with MySQL, Redis, Kafka, RabbitMQ config)

auction-common/src/main/java/com/auction/common/event/
  ticket/ (StockReservedEvent, StockConfirmedEvent, StockReleasedEvent)
  KafkaTopics.java (add TICKET_EVENTS, TICKET_EVENTS_DLT)
  EventTypes.java (add STOCK_RESERVED, STOCK_CONFIRMED, STOCK_RELEASED)
```
