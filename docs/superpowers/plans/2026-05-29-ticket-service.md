# Ticket Service (P3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement ticket-service with Redis Lua oversell prevention, MySQL persistence, Kafka event broadcasting, and RabbitMQ delayed queue for 30-minute reservation timeout.

**Architecture:** Redis serves as the source of truth for available stock via atomic Lua scripts. MySQL persists ticket_stock and reservation records. Kafka broadcasts stock events (reserved/confirmed/released) to downstream services. RabbitMQ delayed-message exchange handles 30-minute reservation expiry with automatic stock rollback.

**Tech Stack:** Java 17, Spring Boot 3.2.5, MyBatis-Plus 3.5.6, Spring Kafka, Spring AMQP (RabbitMQ with delayed-message plugin), Spring Data Redis (Lettuce), MySQL 8.0.

---

## File Map

### auction-common (modify)

| File | Action | Purpose |
|------|--------|---------|
| `auction-common/src/main/java/com/auction/common/event/KafkaTopics.java` | Modify | Add TICKET_EVENTS, TICKET_EVENTS_DLT |
| `auction-common/src/main/java/com/auction/common/event/EventTypes.java` | Modify | Add STOCK_RESERVED, STOCK_CONFIRMED, STOCK_RELEASED |
| `auction-common/src/main/java/com/auction/common/event/ticket/StockReservedEvent.java` | Create | Stock reserved event payload |
| `auction-common/src/main/java/com/auction/common/event/ticket/StockConfirmedEvent.java` | Create | Stock confirmed event payload |
| `auction-common/src/main/java/com/auction/common/event/ticket/StockReleasedEvent.java` | Create | Stock released event payload |

### ticket-service (create/modify)

| File | Action | Purpose |
|------|--------|---------|
| `ticket-service/pom.xml` | Modify | Add Redis, Kafka, RabbitMQ, MyBatis-Plus, MySQL deps |
| `ticket-service/src/main/resources/application.yml` | Modify | Full config: MySQL, Redis, Kafka, RabbitMQ, MyBatis |
| `ticket-service/src/main/resources/lua/reserve_ticket.lua` | Create | Atomic stock deduction script |
| `ticket-service/src/main/resources/lua/release_ticket.lua` | Create | Atomic stock replenishment script |
| `ticket-service/src/main/java/.../domain/entity/TicketStock.java` | Create | Ticket stock entity |
| `ticket-service/src/main/java/.../domain/entity/Reservation.java` | Create | Reservation entity |
| `ticket-service/src/main/java/.../domain/enums/ReservationStatus.java` | Create | PENDING/CONFIRMED/CANCELLED/EXPIRED |
| `ticket-service/src/main/java/.../repository/TicketStockMapper.java` | Create | MyBatis-Plus mapper |
| `ticket-service/src/main/java/.../repository/ReservationMapper.java` | Create | MyBatis-Plus mapper |
| `ticket-service/src/main/java/.../config/RedisConfig.java` | Create | Lua script beans |
| `ticket-service/src/main/java/.../config/KafkaConfig.java` | Create | Topic + error handler |
| `ticket-service/src/main/java/.../config/RabbitMQConfig.java` | Create | Delayed exchange + queue |
| `ticket-service/src/main/java/.../config/MyBatisPlusConfig.java` | Create | Pagination + auto-fill |
| `ticket-service/src/main/java/.../security/UserContextHolder.java` | Create | ThreadLocal user context |
| `ticket-service/src/main/java/.../security/UserContextFilter.java` | Create | Gateway header extraction |
| `ticket-service/src/main/java/.../exception/BusinessException.java` | Create | Business error wrapper |
| `ticket-service/src/main/java/.../exception/GlobalExceptionHandler.java` | Create | Unified error response |
| `ticket-service/src/main/java/.../controller/dto/ReserveRequest.java` | Create | Reserve request DTO |
| `ticket-service/src/main/java/.../controller/dto/ReserveResponse.java` | Create | Reserve response DTO |
| `ticket-service/src/main/java/.../controller/dto/TicketStockResponse.java` | Create | Stock query response DTO |
| `ticket-service/src/main/java/.../controller/dto/CreateTicketRequest.java` | Create | Admin create ticket DTO |
| `ticket-service/src/main/java/.../service/TicketStockService.java` | Create | Service interface |
| `ticket-service/src/main/java/.../service/RedisKeys.java` | Create | Redis key format helpers |
| `ticket-service/src/main/java/.../event/TicketEventProducer.java` | Create | Kafka event publisher |
| `ticket-service/src/main/java/.../service/impl/TicketStockServiceImpl.java` | Create | Core business logic |
| `ticket-service/src/main/java/.../consumer/StockReleaseConsumer.java` | Create | RabbitMQ delayed message handler |
| `ticket-service/src/main/java/.../controller/TicketController.java` | Create | Public endpoints |
| `ticket-service/src/main/java/.../controller/AdminTicketController.java` | Create | Admin endpoints |
| `ticket-service/src/main/java/.../startup/RedisStockWarmer.java` | Create | Startup Redis preload |

---

### Task 1: Add ticket event classes and constants to auction-common

**Files:**
- Create: `auction-common/src/main/java/com/auction/common/event/ticket/StockReservedEvent.java`
- Create: `auction-common/src/main/java/com/auction/common/event/ticket/StockConfirmedEvent.java`
- Create: `auction-common/src/main/java/com/auction/common/event/ticket/StockReleasedEvent.java`
- Modify: `auction-common/src/main/java/com/auction/common/event/KafkaTopics.java`
- Modify: `auction-common/src/main/java/com/auction/common/event/EventTypes.java`

- [ ] **Step 1: Create StockReservedEvent**

```java
package com.auction.common.event.ticket;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StockReservedEvent extends BaseEvent {

    private Long eventId;
    private String ticketType;
    private Long reservationId;
    private Long userId;
    private int quantity;

    public StockReservedEvent(Long eventId, String ticketType, Long reservationId, Long userId, int quantity) {
        super(String.valueOf(eventId), EventTypes.STOCK_RESERVED);
        this.eventId = eventId;
        this.ticketType = ticketType;
        this.reservationId = reservationId;
        this.userId = userId;
        this.quantity = quantity;
    }
}
```

- [ ] **Step 2: Create StockConfirmedEvent**

```java
package com.auction.common.event.ticket;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StockConfirmedEvent extends BaseEvent {

    private Long reservationId;
    private Long userId;

    public StockConfirmedEvent(Long reservationId, Long userId) {
        super(String.valueOf(reservationId), EventTypes.STOCK_CONFIRMED);
        this.reservationId = reservationId;
        this.userId = userId;
    }
}
```

- [ ] **Step 3: Create StockReleasedEvent**

```java
package com.auction.common.event.ticket;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StockReleasedEvent extends BaseEvent {

    private Long reservationId;
    private Long eventId;
    private String ticketType;
    private int quantity;
    private String reason;

    public StockReleasedEvent(Long reservationId, Long eventId, String ticketType, int quantity, String reason) {
        super(String.valueOf(reservationId), EventTypes.STOCK_RELEASED);
        this.reservationId = reservationId;
        this.eventId = eventId;
        this.ticketType = ticketType;
        this.quantity = quantity;
        this.reason = reason;
    }
}
```

- [ ] **Step 4: Update KafkaTopics.java**

Add two new constants to the existing `KafkaTopics.java` class:

```java
public static final String TICKET_EVENTS = "ticket-events";
public static final String TICKET_EVENTS_DLT = "ticket-events-dlt";
```

The full file becomes:

```java
package com.auction.common.event;

public final class KafkaTopics {

    public static final String AUCTION_EVENTS = "auction-events";
    public static final String AUCTION_EVENTS_DLT = "auction-events-dlt";
    public static final String TICKET_EVENTS = "ticket-events";
    public static final String TICKET_EVENTS_DLT = "ticket-events-dlt";

    private KafkaTopics() {
    }
}
```

- [ ] **Step 5: Update EventTypes.java**

Add three new constants to the existing `EventTypes.java` class:

```java
public static final String STOCK_RESERVED = "StockReserved";
public static final String STOCK_CONFIRMED = "StockConfirmed";
public static final String STOCK_RELEASED = "StockReleased";
```

The full file becomes:

```java
package com.auction.common.event;

public final class EventTypes {

    public static final String AUCTION_CREATED = "AuctionCreated";
    public static final String AUCTION_ACTIVATED = "AuctionActivated";
    public static final String BID_PLACED = "BidPlaced";
    public static final String BID_OUTBID = "BidOutbid";
    public static final String AUCTION_SETTLED = "AuctionSettled";
    public static final String AUCTION_EXPIRED = "AuctionExpired";

    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_CONFIRMED = "StockConfirmed";
    public static final String STOCK_RELEASED = "StockReleased";

    private EventTypes() {
    }
}
```

- [ ] **Step 6: Commit**

```bash
git add auction-common/src/main/java/com/auction/common/event/ticket/ auction-common/src/main/java/com/auction/common/event/KafkaTopics.java auction-common/src/main/java/com/auction/common/event/EventTypes.java
git commit -m "feat(common): add ticket event classes and constants"
```

---

### Task 2: Update ticket-service pom.xml and create domain entities

**Files:**
- Modify: `ticket-service/pom.xml`
- Create: `ticket-service/src/main/java/com/auction/ticket/domain/enums/ReservationStatus.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/domain/entity/TicketStock.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/domain/entity/Reservation.java`

- [ ] **Step 1: Update pom.xml with all dependencies**

Replace `ticket-service/pom.xml` with:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.auction</groupId>
        <artifactId>high-concurrency-auction-platform</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>ticket-service</artifactId>
    <name>Ticket Service</name>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-config</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>com.auction</groupId>
            <artifactId>auction-common</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- Data + Cache -->
        <dependency>
            <groupId>com.baomidou</groupId>
            <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
            <version>${mybatis-plus.version}</version>
        </dependency>
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>

        <!-- Messaging -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-amqp</artifactId>
        </dependency>

        <!-- Validation -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Create ReservationStatus enum**

```java
package com.auction.ticket.domain.enums;

public enum ReservationStatus {
    PENDING,
    CONFIRMED,
    CANCELLED,
    EXPIRED
}
```

- [ ] **Step 3: Create TicketStock entity**

```java
package com.auction.ticket.domain.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("ticket_stock")
public class TicketStock {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long eventId;

    private String ticketType;

    private Integer totalQuantity;

    private Integer reservedQuantity;

    private Integer soldQuantity;

    private Integer version;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

- [ ] **Step 4: Create Reservation entity**

```java
package com.auction.ticket.domain.entity;

import com.auction.ticket.domain.enums.ReservationStatus;
import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("reservation")
public class Reservation {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long stockId;

    private Long userId;

    private Integer quantity;

    private ReservationStatus status;

    private LocalDateTime expireAt;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

- [ ] **Step 5: Commit**

```bash
git add ticket-service/pom.xml ticket-service/src/main/java/com/auction/ticket/domain/
git commit -m "feat(ticket): add pom dependencies and domain entities"
```

---

### Task 3: Create repository mappers

**Files:**
- Create: `ticket-service/src/main/java/com/auction/ticket/repository/TicketStockMapper.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/repository/ReservationMapper.java`

- [ ] **Step 1: Create TicketStockMapper**

```java
package com.auction.ticket.repository;

import com.auction.ticket.domain.entity.TicketStock;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface TicketStockMapper extends BaseMapper<TicketStock> {

    @Select("SELECT * FROM ticket_stock WHERE event_id = #{eventId}")
    List<TicketStock> findByEventId(@Param("eventId") Long eventId);

    @Select("SELECT * FROM ticket_stock WHERE event_id = #{eventId} AND ticket_type = #{ticketType}")
    TicketStock findByEventIdAndTicketType(@Param("eventId") Long eventId, @Param("ticketType") String ticketType);

    @Update("UPDATE ticket_stock SET reserved_quantity = reserved_quantity + #{quantity}, version = version + 1 WHERE id = #{id}")
    int incrementReserved(@Param("id") Long id, @Param("quantity") int quantity);

    @Update("UPDATE ticket_stock SET reserved_quantity = reserved_quantity - #{quantity}, sold_quantity = sold_quantity + #{quantity}, version = version + 1 WHERE id = #{id} AND reserved_quantity >= #{quantity}")
    int decrementReservedAndIncrementSold(@Param("id") Long id, @Param("quantity") int quantity);

    @Update("UPDATE ticket_stock SET reserved_quantity = reserved_quantity - #{quantity}, version = version + 1 WHERE id = #{id} AND reserved_quantity >= #{quantity}")
    int decrementReserved(@Param("id") Long id, @Param("quantity") int quantity);
}
```

- [ ] **Step 2: Create ReservationMapper**

```java
package com.auction.ticket.repository;

import com.auction.ticket.domain.entity.Reservation;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface ReservationMapper extends BaseMapper<Reservation> {

    @Select("SELECT * FROM reservation WHERE id = #{id} AND user_id = #{userId}")
    Reservation findByIdAndUserId(@Param("id") Long id, @Param("userId") Long userId);

    @Select("SELECT * FROM reservation WHERE id = #{id}")
    Reservation findById(@Param("id") Long id);

    @Update("UPDATE reservation SET status = #{status} WHERE id = #{id} AND status = 'PENDING'")
    int updateStatusIfPending(@Param("id") Long id, @Param("status") String status);
}
```

- [ ] **Step 3: Commit**

```bash
git add ticket-service/src/main/java/com/auction/ticket/repository/
git commit -m "feat(ticket): add MyBatis-Plus repository mappers"
```

---

### Task 4: Create Lua scripts and infrastructure configs

**Files:**
- Create: `ticket-service/src/main/resources/lua/reserve_ticket.lua`
- Create: `ticket-service/src/main/resources/lua/release_ticket.lua`
- Create: `ticket-service/src/main/java/com/auction/ticket/config/RedisConfig.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/config/KafkaConfig.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/config/RabbitMQConfig.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/config/MyBatisPlusConfig.java`

- [ ] **Step 1: Create reserve_ticket.lua**

```lua
-- KEYS[1] stock key (e.g. stock:1001:VIP)
-- ARGV[1] quantity to reserve
--
-- Returns {ok, code}
--   ok   1 = success, -1 = failure
--   code 'OK' | 'STOCK_NOT_FOUND' | 'OUT_OF_STOCK'

local stockKey = KEYS[1]
local quantity = tonumber(ARGV[1])

local current = tonumber(redis.call('GET', stockKey))
if current == nil then
    return {-1, 'STOCK_NOT_FOUND'}
end

if current < quantity then
    return {-1, 'OUT_OF_STOCK'}
end

redis.call('DECRBY', stockKey, quantity)
return {1, 'OK'}
```

- [ ] **Step 2: Create release_ticket.lua**

```lua
-- KEYS[1] stock key (e.g. stock:1001:VIP)
-- ARGV[1] quantity to release
--
-- Returns {1, 'OK'}

local stockKey = KEYS[1]
local quantity = tonumber(ARGV[1])

redis.call('INCRBY', stockKey, quantity)
return {1, 'OK'}
```

- [ ] **Step 3: Create RedisConfig**

```java
package com.auction.ticket.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;

@Configuration
public class RedisConfig {

    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DefaultRedisScript<List> reserveTicketScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/reserve_ticket.lua"));
        script.setResultType(List.class);
        return script;
    }

    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DefaultRedisScript<List> releaseTicketScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/release_ticket.lua"));
        script.setResultType(List.class);
        return script;
    }
}
```

- [ ] **Step 4: Create KafkaConfig**

```java
package com.auction.ticket.config;

import com.auction.common.event.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic ticketEventsTopic() {
        return TopicBuilder.name(KafkaTopics.TICKET_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ticketEventsDeadLetterTopic() {
        return TopicBuilder.name(KafkaTopics.TICKET_EVENTS_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(
                        KafkaTopics.TICKET_EVENTS_DLT, record.partition()));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }
}
```

- [ ] **Step 5: Create RabbitMQConfig**

```java
package com.auction.ticket.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    public static final String DELAY_EXCHANGE = "delay.exchange";
    public static final String STOCK_RELEASE_QUEUE = "stock-release-queue";
    public static final String STOCK_RELEASE_ROUTING_KEY = "stock.release";

    @Bean
    public CustomExchange delayExchange() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        return new CustomExchange(DELAY_EXCHANGE, "x-delayed-message", true, false, args);
    }

    @Bean
    public Queue stockReleaseQueue() {
        return new Queue(STOCK_RELEASE_QUEUE, true);
    }

    @Bean
    public Binding stockReleaseBinding() {
        return BindingBuilder.bind(stockReleaseQueue())
                .to(delayExchange())
                .with(STOCK_RELEASE_ROUTING_KEY)
                .noargs();
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
```

- [ ] **Step 6: Create MyBatisPlusConfig**

```java
package com.auction.ticket.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.apache.ibatis.reflection.MetaObject;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;

@Configuration
@MapperScan("com.auction.ticket.repository")
public class MyBatisPlusConfig {

    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.MYSQL));
        return interceptor;
    }

    @Bean
    public MetaObjectHandler metaObjectHandler() {
        return new MetaObjectHandler() {
            @Override
            public void insertFill(MetaObject metaObject) {
                this.strictInsertFill(metaObject, "createdAt", LocalDateTime.class, LocalDateTime.now());
                this.strictInsertFill(metaObject, "updatedAt", LocalDateTime.class, LocalDateTime.now());
            }

            @Override
            public void updateFill(MetaObject metaObject) {
                this.strictUpdateFill(metaObject, "updatedAt", LocalDateTime.class, LocalDateTime.now());
            }
        };
    }
}
```

- [ ] **Step 7: Commit**

```bash
git add ticket-service/src/main/resources/lua/ ticket-service/src/main/java/com/auction/ticket/config/
git commit -m "feat(ticket): add Lua scripts and infrastructure configs"
```

---

### Task 5: Add security, exception handling, and DTOs

**Files:**
- Create: `ticket-service/src/main/java/com/auction/ticket/security/UserContextHolder.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/security/UserContextFilter.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/exception/BusinessException.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/exception/GlobalExceptionHandler.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/controller/dto/ReserveRequest.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/controller/dto/ReserveResponse.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/controller/dto/TicketStockResponse.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/controller/dto/CreateTicketRequest.java`

- [ ] **Step 1: Create UserContextHolder**

```java
package com.auction.ticket.security;

import com.auction.common.security.UserContext;

public final class UserContextHolder {

    private static final ThreadLocal<UserContext> CONTEXT = new ThreadLocal<>();

    public static void set(UserContext ctx) {
        CONTEXT.set(ctx);
    }

    public static UserContext get() {
        return CONTEXT.get();
    }

    public static void clear() {
        CONTEXT.remove();
    }

    private UserContextHolder() {
    }
}
```

- [ ] **Step 2: Create UserContextFilter**

```java
package com.auction.ticket.security;

import com.auction.common.security.GatewayHeaders;
import com.auction.common.security.UserContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Component
public class UserContextFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        try {
            String userIdHeader = request.getHeader(GatewayHeaders.USER_ID);
            if (StringUtils.hasText(userIdHeader)) {
                Set<String> roles = parseRoles(request.getHeader(GatewayHeaders.USER_ROLES));
                UserContext ctx = UserContext.builder()
                        .userId(Long.parseLong(userIdHeader))
                        .username(request.getHeader(GatewayHeaders.USERNAME))
                        .roles(roles)
                        .build();
                UserContextHolder.set(ctx);
            }
            filterChain.doFilter(request, response);
        } finally {
            UserContextHolder.clear();
        }
    }

    private Set<String> parseRoles(String rolesHeader) {
        if (!StringUtils.hasText(rolesHeader)) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(rolesHeader.split(",")));
    }
}
```

- [ ] **Step 3: Create BusinessException**

```java
package com.auction.ticket.exception;

import lombok.Getter;

@Getter
public class BusinessException extends RuntimeException {

    private final int code;

    public BusinessException(int code, String message) {
        super(message);
        this.code = code;
    }
}
```

- [ ] **Step 4: Create GlobalExceptionHandler**

```java
package com.auction.ticket.exception;

import com.auction.common.dto.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Void>> handleValidationException(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ResponseEntity.badRequest().body(ApiResponse.error(400, message));
    }

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ApiResponse<Void>> handleBusinessException(BusinessException ex) {
        HttpStatus status = HttpStatus.resolve(ex.getCode());
        if (status == null) {
            status = HttpStatus.BAD_REQUEST;
        }
        return ResponseEntity.status(status).body(ApiResponse.error(ex.getCode(), ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleException(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(500, "Internal server error"));
    }
}
```

- [ ] **Step 5: Create ReserveRequest DTO**

```java
package com.auction.ticket.controller.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ReserveRequest {

    @NotNull(message = "eventId is required")
    private Long eventId;

    @NotBlank(message = "ticketType is required")
    private String ticketType;

    @NotNull(message = "quantity is required")
    @Min(value = 1, message = "quantity must be at least 1")
    private Integer quantity;
}
```

- [ ] **Step 6: Create ReserveResponse DTO**

```java
package com.auction.ticket.controller.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class ReserveResponse {

    private Long reservationId;
    private Long eventId;
    private String ticketType;
    private Integer quantity;
    private LocalDateTime expireAt;
}
```

- [ ] **Step 7: Create TicketStockResponse DTO**

```java
package com.auction.ticket.controller.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TicketStockResponse {

    private Long stockId;
    private Long eventId;
    private String ticketType;
    private Integer totalQuantity;
    private Integer availableQuantity;
    private Integer reservedQuantity;
    private Integer soldQuantity;
}
```

- [ ] **Step 8: Create CreateTicketRequest DTO**

```java
package com.auction.ticket.controller.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class CreateTicketRequest {

    @NotNull(message = "eventId is required")
    private Long eventId;

    @NotBlank(message = "ticketType is required")
    private String ticketType;

    @NotNull(message = "totalQuantity is required")
    @Min(value = 1, message = "totalQuantity must be at least 1")
    private Integer totalQuantity;
}
```

- [ ] **Step 9: Commit**

```bash
git add ticket-service/src/main/java/com/auction/ticket/security/ ticket-service/src/main/java/com/auction/ticket/exception/ ticket-service/src/main/java/com/auction/ticket/controller/dto/
git commit -m "feat(ticket): add security, exception handling, and DTOs"
```

---

### Task 6: Implement service layer and event producer

**Files:**
- Create: `ticket-service/src/main/java/com/auction/ticket/service/RedisKeys.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/event/TicketEventProducer.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/service/TicketStockService.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/service/impl/TicketStockServiceImpl.java`

- [ ] **Step 1: Create RedisKeys**

```java
package com.auction.ticket.service;

public final class RedisKeys {

    public static String stock(Long eventId, String ticketType) {
        return "stock:" + eventId + ":" + ticketType;
    }

    private RedisKeys() {
    }
}
```

- [ ] **Step 2: Create TicketEventProducer**

```java
package com.auction.ticket.event;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.ticket.StockConfirmedEvent;
import com.auction.common.event.ticket.StockReleasedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TicketEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TicketEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishStockReserved(Long eventId, String ticketType, Long reservationId, Long userId, int quantity) {
        StockReservedEvent event = new StockReservedEvent(eventId, ticketType, reservationId, userId, quantity);
        kafkaTemplate.send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }

    public void publishStockConfirmed(Long reservationId, Long userId) {
        StockConfirmedEvent event = new StockConfirmedEvent(reservationId, userId);
        kafkaTemplate.send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }

    public void publishStockReleased(Long reservationId, Long eventId, String ticketType, int quantity, String reason) {
        StockReleasedEvent event = new StockReleasedEvent(reservationId, eventId, ticketType, quantity, reason);
        kafkaTemplate.send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }
}
```

- [ ] **Step 3: Create TicketStockService interface**

```java
package com.auction.ticket.service;

import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.controller.dto.TicketStockResponse;

import java.util.List;

public interface TicketStockService {

    List<TicketStockResponse> getStockByEvent(Long eventId);

    ReserveResponse reserve(Long userId, ReserveRequest request);

    void confirm(Long userId, Long reservationId);

    void cancel(Long userId, Long reservationId);

    TicketStockResponse createTicketStock(CreateTicketRequest request);
}
```

- [ ] **Step 4: Create TicketStockServiceImpl**

```java
package com.auction.ticket.service.impl;

import com.auction.ticket.config.RabbitMQConfig;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.controller.dto.TicketStockResponse;
import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.domain.enums.ReservationStatus;
import com.auction.ticket.event.TicketEventProducer;
import com.auction.ticket.exception.BusinessException;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import com.auction.ticket.service.TicketStockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class TicketStockServiceImpl implements TicketStockService {

    private static final Logger log = LoggerFactory.getLogger(TicketStockServiceImpl.class);
    private static final int RESERVATION_TIMEOUT_MINUTES = 30;

    private final AtomicBoolean ready = new AtomicBoolean(false);

    private final StringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> reserveTicketScript;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> releaseTicketScript;
    private final TicketStockMapper stockMapper;
    private final ReservationMapper reservationMapper;
    private final TicketEventProducer eventProducer;
    private final RabbitTemplate rabbitTemplate;

    @SuppressWarnings("rawtypes")
    public TicketStockServiceImpl(StringRedisTemplate redis,
                                  DefaultRedisScript<List> reserveTicketScript,
                                  DefaultRedisScript<List> releaseTicketScript,
                                  TicketStockMapper stockMapper,
                                  ReservationMapper reservationMapper,
                                  TicketEventProducer eventProducer,
                                  RabbitTemplate rabbitTemplate) {
        this.redis = redis;
        this.reserveTicketScript = reserveTicketScript;
        this.releaseTicketScript = releaseTicketScript;
        this.stockMapper = stockMapper;
        this.reservationMapper = reservationMapper;
        this.eventProducer = eventProducer;
        this.rabbitTemplate = rabbitTemplate;
    }

    public void markReady() {
        ready.set(true);
    }

    @Override
    public List<TicketStockResponse> getStockByEvent(Long eventId) {
        return stockMapper.findByEventId(eventId).stream()
                .map(this::toStockResponse)
                .toList();
    }

    @Override
    @SuppressWarnings("unchecked")
    @Transactional
    public ReserveResponse reserve(Long userId, ReserveRequest request) {
        if (!ready.get()) {
            throw new BusinessException(503, "Service is initializing, please retry");
        }

        String stockKey = RedisKeys.stock(request.getEventId(), request.getTicketType());

        // 1. Redis Lua atomic deduction
        List<Object> result;
        try {
            result = (List<Object>) redis.execute(
                    reserveTicketScript,
                    List.of(stockKey),
                    String.valueOf(request.getQuantity()));
        } catch (Exception e) {
            log.error("Redis execution failed for stock key={}", stockKey, e);
            throw new BusinessException(500, "Stock service unavailable");
        }

        if (result == null || result.size() < 2) {
            throw new BusinessException(500, "Stock evaluation failed");
        }

        long okFlag = ((Number) result.get(0)).longValue();
        if (okFlag != 1L) {
            String code = (String) result.get(1);
            if ("OUT_OF_STOCK".equals(code)) {
                throw new BusinessException(400, "Out of stock");
            }
            throw new BusinessException(404, "Ticket stock not found");
        }

        // 2. MySQL: lookup stock, create reservation, update reserved_quantity
        try {
            TicketStock stock = stockMapper.findByEventIdAndTicketType(
                    request.getEventId(), request.getTicketType());
            if (stock == null) {
                rollbackRedis(stockKey, request.getQuantity());
                throw new BusinessException(404, "Ticket stock not found");
            }

            Reservation reservation = new Reservation();
            reservation.setStockId(stock.getId());
            reservation.setUserId(userId);
            reservation.setQuantity(request.getQuantity());
            reservation.setStatus(ReservationStatus.PENDING);
            reservation.setExpireAt(LocalDateTime.now().plusMinutes(RESERVATION_TIMEOUT_MINUTES));
            reservationMapper.insert(reservation);

            stockMapper.incrementReserved(stock.getId(), request.getQuantity());

            // 3. Kafka event
            eventProducer.publishStockReserved(
                    request.getEventId(), request.getTicketType(),
                    reservation.getId(), userId, request.getQuantity());

            // 4. RabbitMQ delayed message (30 min)
            rabbitTemplate.convertAndSend(
                    RabbitMQConfig.DELAY_EXCHANGE,
                    RabbitMQConfig.STOCK_RELEASE_ROUTING_KEY,
                    reservation.getId(),
                    msg -> {
                        msg.getMessageProperties().setDelay(RESERVATION_TIMEOUT_MINUTES * 60 * 1000);
                        return msg;
                    });

            log.info("Reserved {} tickets for user={}, event={}, type={}, reservationId={}",
                    request.getQuantity(), userId, request.getEventId(),
                    request.getTicketType(), reservation.getId());

            return ReserveResponse.builder()
                    .reservationId(reservation.getId())
                    .eventId(request.getEventId())
                    .ticketType(request.getTicketType())
                    .quantity(request.getQuantity())
                    .expireAt(reservation.getExpireAt())
                    .build();
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            rollbackRedis(stockKey, request.getQuantity());
            log.error("MySQL operation failed after Redis deduction, rolled back Redis for key={}", stockKey, e);
            throw new BusinessException(500, "Reservation failed");
        }
    }

    @Override
    @Transactional
    public void confirm(Long userId, Long reservationId) {
        Reservation reservation = reservationMapper.findByIdAndUserId(reservationId, userId);
        if (reservation == null) {
            throw new BusinessException(404, "Reservation not found");
        }
        if (reservation.getStatus() != ReservationStatus.PENDING) {
            throw new BusinessException(400, "Reservation is not in PENDING status");
        }

        reservationMapper.updateStatusIfPending(reservationId, ReservationStatus.CONFIRMED.name());
        stockMapper.decrementReservedAndIncrementSold(reservation.getStockId(), reservation.getQuantity());

        eventProducer.publishStockConfirmed(reservationId, userId);

        log.info("Confirmed reservation id={}, userId={}", reservationId, userId);
    }

    @Override
    @Transactional
    public void cancel(Long userId, Long reservationId) {
        Reservation reservation = reservationMapper.findByIdAndUserId(reservationId, userId);
        if (reservation == null) {
            throw new BusinessException(404, "Reservation not found");
        }
        if (reservation.getStatus() != ReservationStatus.PENDING) {
            throw new BusinessException(400, "Reservation is not in PENDING status");
        }

        reservationMapper.updateStatusIfPending(reservationId, ReservationStatus.CANCELLED.name());
        stockMapper.decrementReserved(reservation.getStockId(), reservation.getQuantity());

        // Look up stock to get eventId and ticketType for Redis rollback
        TicketStock stock = stockMapper.selectById(reservation.getStockId());
        if (stock != null) {
            String stockKey = RedisKeys.stock(stock.getEventId(), stock.getTicketType());
            rollbackRedis(stockKey, reservation.getQuantity());
            eventProducer.publishStockReleased(
                    reservationId, stock.getEventId(), stock.getTicketType(),
                    reservation.getQuantity(), "CANCELLED");
        }

        log.info("Cancelled reservation id={}, userId={}", reservationId, userId);
    }

    @Override
    @Transactional
    public TicketStockResponse createTicketStock(CreateTicketRequest request) {
        TicketStock stock = new TicketStock();
        stock.setEventId(request.getEventId());
        stock.setTicketType(request.getTicketType());
        stock.setTotalQuantity(request.getTotalQuantity());
        stock.setReservedQuantity(0);
        stock.setSoldQuantity(0);
        stock.setVersion(0);
        stockMapper.insert(stock);

        // Warm up Redis
        String stockKey = RedisKeys.stock(request.getEventId(), request.getTicketType());
        redis.opsForValue().set(stockKey, String.valueOf(request.getTotalQuantity()));

        log.info("Created ticket stock: eventId={}, type={}, total={}",
                request.getEventId(), request.getTicketType(), request.getTotalQuantity());

        return toStockResponse(stock);
    }

    @SuppressWarnings("unchecked")
    private void rollbackRedis(String stockKey, int quantity) {
        try {
            redis.execute(releaseTicketScript, List.of(stockKey), String.valueOf(quantity));
        } catch (Exception e) {
            log.error("Redis rollback failed for key={}, quantity={}", stockKey, quantity, e);
        }
    }

    private TicketStockResponse toStockResponse(TicketStock stock) {
        int available = stock.getTotalQuantity() - stock.getReservedQuantity() - stock.getSoldQuantity();
        return TicketStockResponse.builder()
                .stockId(stock.getId())
                .eventId(stock.getEventId())
                .ticketType(stock.getTicketType())
                .totalQuantity(stock.getTotalQuantity())
                .availableQuantity(available)
                .reservedQuantity(stock.getReservedQuantity())
                .soldQuantity(stock.getSoldQuantity())
                .build();
    }
}
```

- [ ] **Step 5: Commit**

```bash
git add ticket-service/src/main/java/com/auction/ticket/service/ ticket-service/src/main/java/com/auction/ticket/event/
git commit -m "feat(ticket): implement core service layer with Redis Lua and event producer"
```

---

### Task 7: Implement RabbitMQ delayed queue consumer

**Files:**
- Create: `ticket-service/src/main/java/com/auction/ticket/consumer/StockReleaseConsumer.java`

- [ ] **Step 1: Create StockReleaseConsumer**

```java
package com.auction.ticket.consumer;

import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.domain.enums.ReservationStatus;
import com.auction.ticket.event.TicketEventProducer;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import com.auction.ticket.service.impl.TicketStockServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class StockReleaseConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockReleaseConsumer.class);

    private final ReservationMapper reservationMapper;
    private final TicketStockMapper stockMapper;
    private final StringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> releaseTicketScript;
    private final TicketEventProducer eventProducer;

    @SuppressWarnings("rawtypes")
    public StockReleaseConsumer(ReservationMapper reservationMapper,
                                TicketStockMapper stockMapper,
                                StringRedisTemplate redis,
                                DefaultRedisScript<List> releaseTicketScript,
                                TicketEventProducer eventProducer) {
        this.reservationMapper = reservationMapper;
        this.stockMapper = stockMapper;
        this.redis = redis;
        this.releaseTicketScript = releaseTicketScript;
        this.eventProducer = eventProducer;
    }

    @RabbitListener(queues = "stock-release-queue")
    @Transactional
    public void handleStockRelease(Long reservationId) {
        log.info("Received stock release delayed message for reservationId={}", reservationId);

        Reservation reservation = reservationMapper.findById(reservationId);
        if (reservation == null) {
            log.warn("Reservation not found: id={}", reservationId);
            return;
        }

        if (reservation.getStatus() != ReservationStatus.PENDING) {
            log.info("Reservation already processed: id={}, status={}", reservationId, reservation.getStatus());
            return;
        }

        int updated = reservationMapper.updateStatusIfPending(reservationId, ReservationStatus.EXPIRED.name());
        if (updated == 0) {
            log.info("Reservation status race: id={}, another thread handled it", reservationId);
            return;
        }

        stockMapper.decrementReserved(reservation.getStockId(), reservation.getQuantity());

        TicketStock stock = stockMapper.selectById(reservation.getStockId());
        if (stock != null) {
            String stockKey = RedisKeys.stock(stock.getEventId(), stock.getTicketType());
            try {
                @SuppressWarnings("unchecked")
                List<Object> result = (List<Object>) redis.execute(
                        releaseTicketScript, List.of(stockKey), String.valueOf(reservation.getQuantity()));
                log.info("Redis stock rolled back: key={}, quantity={}", stockKey, reservation.getQuantity());
            } catch (Exception e) {
                log.error("Redis rollback failed for delayed release, key={}", stockKey, e);
            }

            eventProducer.publishStockReleased(
                    reservationId, stock.getEventId(), stock.getTicketType(),
                    reservation.getQuantity(), "TIMEOUT");
        }

        log.info("Expired reservation: id={}", reservationId);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add ticket-service/src/main/java/com/auction/ticket/consumer/
git commit -m "feat(ticket): add RabbitMQ delayed queue consumer for stock timeout rollback"
```

---

### Task 8: Implement controllers

**Files:**
- Create: `ticket-service/src/main/java/com/auction/ticket/controller/TicketController.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/controller/AdminTicketController.java`

- [ ] **Step 1: Create TicketController**

```java
package com.auction.ticket.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.controller.dto.TicketStockResponse;
import com.auction.ticket.security.UserContextHolder;
import com.auction.ticket.service.TicketStockService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/tickets")
public class TicketController {

    private final TicketStockService ticketStockService;

    public TicketController(TicketStockService ticketStockService) {
        this.ticketStockService = ticketStockService;
    }

    @GetMapping("/events/{eventId}")
    public ResponseEntity<ApiResponse<List<TicketStockResponse>>> getStockByEvent(@PathVariable Long eventId) {
        return ResponseEntity.ok(ApiResponse.success(ticketStockService.getStockByEvent(eventId)));
    }

    @PostMapping("/reserve")
    public ResponseEntity<ApiResponse<ReserveResponse>> reserve(@Valid @RequestBody ReserveRequest request) {
        UserContext ctx = UserContextHolder.get();
        ReserveResponse response = ticketStockService.reserve(ctx.getUserId(), request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @PostMapping("/{reservationId}/confirm")
    public ResponseEntity<ApiResponse<Void>> confirm(@PathVariable Long reservationId) {
        UserContext ctx = UserContextHolder.get();
        ticketStockService.confirm(ctx.getUserId(), reservationId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    @DeleteMapping("/{reservationId}")
    public ResponseEntity<ApiResponse<Void>> cancel(@PathVariable Long reservationId) {
        UserContext ctx = UserContextHolder.get();
        ticketStockService.cancel(ctx.getUserId(), reservationId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }
}
```

- [ ] **Step 2: Create AdminTicketController**

```java
package com.auction.ticket.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.TicketStockResponse;
import com.auction.ticket.security.UserContextHolder;
import com.auction.ticket.service.TicketStockService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/tickets")
public class AdminTicketController {

    private final TicketStockService ticketStockService;

    public AdminTicketController(TicketStockService ticketStockService) {
        this.ticketStockService = ticketStockService;
    }

    @PostMapping
    public ResponseEntity<ApiResponse<TicketStockResponse>> createTicketStock(
            @Valid @RequestBody CreateTicketRequest request) {
        UserContext ctx = UserContextHolder.get();
        if (!ctx.isAdmin()) {
            return ResponseEntity.status(403)
                    .body(ApiResponse.error(403, "Admin role required"));
        }
        TicketStockResponse response = ticketStockService.createTicketStock(request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add ticket-service/src/main/java/com/auction/ticket/controller/
git commit -m "feat(ticket): add public and admin REST controllers"
```

---

### Task 9: Add startup warmer and update application.yml

**Files:**
- Create: `ticket-service/src/main/java/com/auction/ticket/startup/RedisStockWarmer.java`
- Modify: `ticket-service/src/main/resources/application.yml`

- [ ] **Step 1: Create RedisStockWarmer**

```java
package com.auction.ticket.startup;

import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import com.auction.ticket.service.impl.TicketStockServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RedisStockWarmer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RedisStockWarmer.class);

    private final TicketStockMapper stockMapper;
    private final StringRedisTemplate redis;
    private final TicketStockServiceImpl ticketStockService;

    public RedisStockWarmer(TicketStockMapper stockMapper,
                            StringRedisTemplate redis,
                            TicketStockServiceImpl ticketStockService) {
        this.stockMapper = stockMapper;
        this.redis = redis;
        this.ticketStockService = ticketStockService;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Warming up Redis stock cache from MySQL...");
        try {
            List<TicketStock> stocks = stockMapper.selectList(null);
            for (TicketStock stock : stocks) {
                int available = stock.getTotalQuantity() - stock.getReservedQuantity() - stock.getSoldQuantity();
                String key = RedisKeys.stock(stock.getEventId(), stock.getTicketType());
                redis.opsForValue().set(key, String.valueOf(available));
            }
            ticketStockService.markReady();
            log.info("Redis stock cache warmed up: {} stock entries loaded", stocks.size());
        } catch (Exception e) {
            log.error("Failed to warm up Redis stock cache", e);
            // Still mark ready so the service doesn't stay blocked forever
            // Individual Redis operations will fail and return errors
            ticketStockService.markReady();
        }
    }
}
```

- [ ] **Step 2: Update application.yml**

Replace `ticket-service/src/main/resources/application.yml` with:

```yaml
server:
  port: 8082

spring:
  cloud:
    nacos:
      discovery:
        server-addr: localhost:8848
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://localhost:3306/ticket_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true
    username: root
    password: root
  data:
    redis:
      host: localhost
      port: 6379
      timeout: 200ms
      lettuce:
        pool:
          max-active: 32
          max-idle: 16
          min-idle: 4
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      enable-idempotence: true
      retries: 5
      properties:
        max.in.flight.requests.per.connection: 5
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: ticket-service
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "com.auction.common.event,com.auction.common.event.ticket"
    listener:
      ack-mode: manual_immediate
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: guest

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics

mybatis-plus:
  mapper-locations: classpath:/mapper/**/*.xml
  configuration:
    map-underscore-to-camel-case: true
    default-enum-type-handler: com.baomidou.mybatisplus.core.handlers.MybatisEnumTypeHandler

logging:
  level:
    com.auction.ticket: INFO
```

- [ ] **Step 3: Commit**

```bash
git add ticket-service/src/main/java/com/auction/ticket/startup/ ticket-service/src/main/resources/application.yml
git commit -m "feat(ticket): add Redis stock warmer and full application config"
```

---

### Task 10: Build verification and final commit

- [ ] **Step 1: Run Maven build from project root**

```bash
cd D:/codeproject/高并发分布式事件驱动系统 && mvn clean compile -pl auction-common,ticket-service -am
```

Expected: BUILD SUCCESS

- [ ] **Step 2: Fix any compilation errors**

If the build fails, read the error output, fix the issues, and re-run.

- [ ] **Step 3: Run full project build**

```bash
cd D:/codeproject/高并发分布式事件驱动系统 && mvn clean compile
```

Expected: BUILD SUCCESS across all modules.

- [ ] **Step 4: Amend the last commit if fixes were needed, otherwise skip**

---

## Self-Review Checklist

- [x] **Spec coverage:** All API endpoints (GET stock, POST reserve, POST confirm, DELETE cancel, POST admin create) are implemented in Tasks 6-8.
- [x] **Redis Lua scripts:** reserve_ticket.lua and release_ticket.lua in Task 4, used in Task 6 service and Task 7 consumer.
- [x] **Kafka events:** StockReservedEvent, StockConfirmedEvent, StockReleasedEvent in Task 1, producer in Task 6.
- [x] **RabbitMQ delayed queue:** Config in Task 4, consumer in Task 7.
- [x] **Startup warmer:** Task 9.
- [x] **No placeholders:** All code blocks are complete.
- [x] **Type consistency:** Method signatures match across service interface (Task 6), implementation (Task 6), and controllers (Task 8).
