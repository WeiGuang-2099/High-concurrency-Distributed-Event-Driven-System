# Order Service & Distributed Transactions (P4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement order-service with state machine, mock payment, Kafka events, RabbitMQ order timeout, and integrate Seata AT distributed transactions across auction/ticket/order services for atomic settlement.

**Architecture:** order-service manages the order lifecycle (CREATED -> PAYING -> PAID -> COMPLETED / CANCELLED / EXPIRED). Mock payment is synchronous within the pay HTTP call. RabbitMQ delayed messages trigger 30-min order timeout cancellation. Seata AT mode coordinates the auction settlement flow: auction-service (TM) calls ticket-service and order-service (RMs) via OpenFeign within a `@GlobalTransactional`, ensuring atomic stock reservation + order creation. Seata XID propagates via a Feign interceptor on all outgoing calls.

**Tech Stack:** Java 17, Spring Boot 3.2.5, Spring Cloud 2023.0.1, Spring Cloud Alibaba 2022.0.0.0, MyBatis-Plus 3.5.6, Spring Kafka, Spring AMQP (RabbitMQ), OpenFeign, Seata AT 1.8.0, MySQL 8.0.

---

## File Map

### auction-common (modify)

| File | Action | Purpose |
|------|--------|---------|
| `auction-common/src/main/java/com/auction/common/event/EventTypes.java` | Modify | Add order event types |
| `auction-common/src/main/java/com/auction/common/event/KafkaTopics.java` | Modify | Add ORDER_EVENTS, ORDER_EVENTS_DLT |
| `auction-common/src/main/java/com/auction/common/event/order/OrderCreatedEvent.java` | Create | Order created event payload |
| `auction-common/src/main/java/com/auction/common/event/order/PaymentInitiatedEvent.java` | Create | Payment initiated event payload |
| `auction-common/src/main/java/com/auction/common/event/order/PaymentCompletedEvent.java` | Create | Payment completed event payload |
| `auction-common/src/main/java/com/auction/common/event/order/OrderCancelledEvent.java` | Create | Order cancelled event payload |
| `auction-common/src/main/java/com/auction/common/event/order/OrderExpiredEvent.java` | Create | Order expired event payload |
| `auction-common/src/main/java/com/auction/common/feign/SeataFeignInterceptor.java` | Create | Propagate Seata XID in Feign headers |

### order-service (create -- full implementation)

| File | Action | Purpose |
|------|--------|---------|
| `order-service/pom.xml` | Modify | Add MySQL, Redis, Kafka, RabbitMQ, MyBatis-Plus, OpenFeign, Seata deps |
| `order-service/src/main/resources/application.yml` | Modify | Full config: MySQL, Kafka, RabbitMQ, MyBatis, Seata, Feign |
| `order-service/src/main/resources/bootstrap.yml` | Modify | Add shared-configs |
| `order-service/src/main/java/.../domain/enums/OrderType.java` | Create | AUCTION / TICKET enum |
| `order-service/src/main/java/.../domain/enums/OrderStatus.java` | Create | CREATED / PAYING / PAID / COMPLETED / CANCELLED / EXPIRED |
| `order-service/src/main/java/.../domain/enums/PaymentStatus.java` | Create | PENDING / SUCCESS / FAILED |
| `order-service/src/main/java/.../domain/entity/Order.java` | Create | Order entity |
| `order-service/src/main/java/.../domain/entity/Payment.java` | Create | Payment entity |
| `order-service/src/main/java/.../domain/entity/IdempotencyLog.java` | Create | Idempotency log entity |
| `order-service/src/main/java/.../repository/OrderMapper.java` | Create | MyBatis-Plus mapper |
| `order-service/src/main/java/.../repository/PaymentMapper.java` | Create | MyBatis-Plus mapper |
| `order-service/src/main/java/.../repository/IdempotencyLogMapper.java` | Create | MyBatis-Plus mapper |
| `order-service/src/main/java/.../config/MyBatisPlusConfig.java` | Create | Pagination + auto-fill |
| `order-service/src/main/java/.../config/KafkaConfig.java` | Create | Topic + error handler |
| `order-service/src/main/java/.../config/RabbitMQConfig.java` | Create | Delayed exchange + order timeout queue |
| `order-service/src/main/java/.../config/SeataConfig.java` | Create | Seata client datasource proxy |
| `order-service/src/main/java/.../config/FeignConfig.java` | Create | Register SeataFeignInterceptor |
| `order-service/src/main/java/.../client/TicketFeignClient.java` | Create | OpenFeign client for ticket-service |
| `order-service/src/main/java/.../controller/dto/CreateOrderRequest.java` | Create | Create order request DTO |
| `order-service/src/main/java/.../controller/dto/OrderResponse.java` | Create | Order detail response DTO |
| `order-service/src/main/java/.../controller/dto/PayResponse.java` | Create | Payment result DTO |
| `order-service/src/main/java/.../event/OrderEventProducer.java` | Create | Kafka event publisher |
| `order-service/src/main/java/.../service/OrderService.java` | Create | Service interface |
| `order-service/src/main/java/.../service/impl/OrderServiceImpl.java` | Create | Core business logic + state machine |
| `order-service/src/main/java/.../consumer/OrderTimeoutConsumer.java` | Create | RabbitMQ delayed message handler |
| `order-service/src/main/java/.../controller/OrderController.java` | Create | Public endpoints |
| `order-service/src/main/java/.../controller/InternalOrderController.java` | Create | Internal endpoints for Seata |

### auction-service (modify -- Seata + Feign integration)

| File | Action | Purpose |
|------|--------|---------|
| `auction-service/pom.xml` | Modify | Add OpenFeign, Seata, Spring Cloud LoadBalancer deps |
| `auction-service/src/main/resources/application.yml` | Modify | Add Seata config, Feign config |
| `auction-service/src/main/java/.../config/SeataConfig.java` | Create | Seata client datasource proxy |
| `auction-service/src/main/java/.../config/FeignConfig.java` | Create | Register SeataFeignInterceptor |
| `auction-service/src/main/java/.../client/TicketFeignClient.java` | Create | OpenFeign client for ticket-service |
| `auction-service/src/main/java/.../client/OrderFeignClient.java` | Create | OpenFeign client for order-service |
| `auction-service/src/main/java/.../scheduler/AuctionLifecycleScheduler.java` | Modify | Replace Kafka-only settlement with Seata 2PC |
| `auction-service/src/main/java/.../service/SettlementService.java` | Create | Settlement logic with @GlobalTransactional |

### ticket-service (modify -- internal endpoint for Seata)

| File | Action | Purpose |
|------|--------|---------|
| `ticket-service/pom.xml` | Modify | Add OpenFeign, Seata, Spring Cloud LoadBalancer deps |
| `ticket-service/src/main/resources/application.yml` | Modify | Add Seata config, Feign config |
| `ticket-service/src/main/java/.../config/SeataConfig.java` | Create | Seata client datasource proxy |
| `ticket-service/src/main/java/.../config/FeignConfig.java` | Create | Register SeataFeignInterceptor |
| `ticket-service/src/main/java/.../controller/InternalTicketController.java` | Create | Internal settle-reserve endpoint |
| `ticket-service/src/main/java/.../service/TicketStockService.java` | Modify | Add settleReserve method |
| `ticket-service/src/main/java/.../service/impl/TicketStockServiceImpl.java` | Modify | Implement settleReserve (MySQL-only) |

---

### Task 1: Add order event classes and constants to auction-common

**Files:**
- Create: `auction-common/src/main/java/com/auction/common/event/order/OrderCreatedEvent.java`
- Create: `auction-common/src/main/java/com/auction/common/event/order/PaymentInitiatedEvent.java`
- Create: `auction-common/src/main/java/com/auction/common/event/order/PaymentCompletedEvent.java`
- Create: `auction-common/src/main/java/com/auction/common/event/order/OrderCancelledEvent.java`
- Create: `auction-common/src/main/java/com/auction/common/event/order/OrderExpiredEvent.java`
- Modify: `auction-common/src/main/java/com/auction/common/event/KafkaTopics.java`
- Modify: `auction-common/src/main/java/com/auction/common/event/EventTypes.java`

- [ ] **Step 1: Create OrderCreatedEvent**

```java
package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OrderCreatedEvent extends BaseEvent {

    private Long orderId;
    private Long userId;
    private String orderType;
    private Long referenceId;
    private BigDecimal amount;

    public OrderCreatedEvent(Long orderId, Long userId, String orderType,
                             Long referenceId, BigDecimal amount) {
        super(String.valueOf(orderId), EventTypes.ORDER_CREATED);
        this.orderId = orderId;
        this.userId = userId;
        this.orderType = orderType;
        this.referenceId = referenceId;
        this.amount = amount;
    }
}
```

- [ ] **Step 2: Create PaymentInitiatedEvent**

```java
package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class PaymentInitiatedEvent extends BaseEvent {

    private Long orderId;
    private Long userId;
    private BigDecimal amount;

    public PaymentInitiatedEvent(Long orderId, Long userId, BigDecimal amount) {
        super(String.valueOf(orderId), EventTypes.PAYMENT_INITIATED);
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }
}
```

- [ ] **Step 3: Create PaymentCompletedEvent**

```java
package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class PaymentCompletedEvent extends BaseEvent {

    private Long orderId;
    private Long userId;
    private BigDecimal amount;

    public PaymentCompletedEvent(Long orderId, Long userId, BigDecimal amount) {
        super(String.valueOf(orderId), EventTypes.PAYMENT_COMPLETED);
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }
}
```

- [ ] **Step 4: Create OrderCancelledEvent**

```java
package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OrderCancelledEvent extends BaseEvent {

    private Long orderId;
    private Long userId;
    private String reason;

    public OrderCancelledEvent(Long orderId, Long userId, String reason) {
        super(String.valueOf(orderId), EventTypes.ORDER_CANCELLED);
        this.orderId = orderId;
        this.userId = userId;
        this.reason = reason;
    }
}
```

- [ ] **Step 5: Create OrderExpiredEvent**

```java
package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OrderExpiredEvent extends BaseEvent {

    private Long orderId;
    private Long userId;

    public OrderExpiredEvent(Long orderId, Long userId) {
        super(String.valueOf(orderId), EventTypes.ORDER_EXPIRED);
        this.orderId = orderId;
        this.userId = userId;
    }
}
```

- [ ] **Step 6: Update KafkaTopics.java**

Add two new constants:

```java
public static final String ORDER_EVENTS = "order-events";
public static final String ORDER_EVENTS_DLT = "order-events-dlt";
```

The full file becomes:

```java
package com.auction.common.event;

public final class KafkaTopics {

    public static final String AUCTION_EVENTS = "auction-events";
    public static final String AUCTION_EVENTS_DLT = "auction-events-dlt";
    public static final String TICKET_EVENTS = "ticket-events";
    public static final String TICKET_EVENTS_DLT = "ticket-events-dlt";
    public static final String ORDER_EVENTS = "order-events";
    public static final String ORDER_EVENTS_DLT = "order-events-dlt";

    private KafkaTopics() {
    }
}
```

- [ ] **Step 7: Update EventTypes.java**

Add five new constants:

```java
public static final String ORDER_CREATED = "OrderCreated";
public static final String PAYMENT_INITIATED = "PaymentInitiated";
public static final String PAYMENT_COMPLETED = "PaymentCompleted";
public static final String ORDER_CANCELLED = "OrderCancelled";
public static final String ORDER_EXPIRED = "OrderExpired";
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
    public static final String TICKET_CREATED = "TicketCreated";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_CONFIRMED = "StockConfirmed";
    public static final String STOCK_RELEASED = "StockReleased";
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String PAYMENT_INITIATED = "PaymentInitiated";
    public static final String PAYMENT_COMPLETED = "PaymentCompleted";
    public static final String ORDER_CANCELLED = "OrderCancelled";
    public static final String ORDER_EXPIRED = "OrderExpired";

    private EventTypes() {
    }
}
```

- [ ] **Step 8: Commit**

```bash
git add auction-common/src/main/java/com/auction/common/event/order/ auction-common/src/main/java/com/auction/common/event/KafkaTopics.java auction-common/src/main/java/com/auction/common/event/EventTypes.java
git commit -m "feat(common): add order event classes, topics, and event type constants"
```

---

### Task 2: Create SeataFeignInterceptor in auction-common

**Files:**
- Create: `auction-common/src/main/java/com/auction/common/feign/SeataFeignInterceptor.java`

This interceptor propagates the Seata XID to downstream services via Feign. It reads `RootContext.getXID()` and adds it as a header on all outgoing Feign requests.

- [ ] **Step 1: Add Feign dependency to auction-common pom.xml**

Read `auction-common/pom.xml` and add this dependency:

```xml
<dependency>
    <groupId>io.github.openfeign</groupId>
    <artifactId>feign-core</artifactId>
    <scope>provided</scope>
</dependency>
```

Using `<scope>provided</scope>` so auction-common doesn't force Feign on services that don't use it (like notification-service). The actual Feign dependency comes from each service's own pom.

- [ ] **Step 2: Create SeataFeignInterceptor**

```java
package com.auction.common.feign;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import io.seata.core.context.RootContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeataFeignInterceptor implements RequestInterceptor {

    private static final Logger log = LoggerFactory.getLogger(SeataFeignInterceptor.class);

    @Override
    public void apply(RequestTemplate template) {
        String xid = RootContext.getXID();
        if (xid != null && !xid.isEmpty()) {
            template.header("TX_XID", xid);
            log.debug("Propagated Seata XID={} to Feign request", xid);
        }
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add auction-common/pom.xml auction-common/src/main/java/com/auction/common/feign/
git commit -m "feat(common): add SeataFeignInterceptor for XID propagation"
```

---

### Task 3: Set up order-service pom.xml and domain entities

**Files:**
- Modify: `order-service/pom.xml`
- Modify: `order-service/src/main/resources/bootstrap.yml`
- Create: `order-service/src/main/java/com/auction/order/domain/enums/OrderType.java`
- Create: `order-service/src/main/java/com/auction/order/domain/enums/OrderStatus.java`
- Create: `order-service/src/main/java/com/auction/order/domain/enums/PaymentStatus.java`
- Create: `order-service/src/main/java/com/auction/order/domain/entity/Order.java`
- Create: `order-service/src/main/java/com/auction/order/domain/entity/Payment.java`
- Create: `order-service/src/main/java/com/auction/order/domain/entity/IdempotencyLog.java`

- [ ] **Step 1: Replace order-service/pom.xml**

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

    <artifactId>order-service</artifactId>
    <name>Order Service</name>

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

        <!-- Data -->
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

        <!-- Messaging -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-amqp</artifactId>
        </dependency>

        <!-- OpenFeign -->
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-openfeign</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-loadbalancer</artifactId>
        </dependency>

        <!-- Seata -->
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-seata</artifactId>
            <exclusions>
                <exclusion>
                    <groupId>io.seata</groupId>
                    <artifactId>seata-spring-boot-starter</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>io.seata</groupId>
            <artifactId>seata-spring-boot-starter</artifactId>
            <version>${seata.version}</version>
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

- [ ] **Step 2: Update bootstrap.yml with shared-configs**

Replace `order-service/src/main/resources/bootstrap.yml` with:

```yaml
spring:
  application:
    name: order-service
  cloud:
    nacos:
      config:
        server-addr: localhost:8848
        file-extension: yml
        shared-configs:
          - data-id: shared-db.yml
            refresh: true
          - data-id: shared-redis.yml
            refresh: true
          - data-id: shared-kafka.yml
            refresh: true
```

- [ ] **Step 3: Create OrderType enum**

```java
package com.auction.order.domain.enums;

public enum OrderType {
    AUCTION,
    TICKET
}
```

- [ ] **Step 4: Create OrderStatus enum**

```java
package com.auction.order.domain.enums;

public enum OrderStatus {
    CREATED,
    PAYING,
    PAID,
    COMPLETED,
    CANCELLED,
    EXPIRED
}
```

- [ ] **Step 5: Create PaymentStatus enum**

```java
package com.auction.order.domain.enums;

public enum PaymentStatus {
    PENDING,
    SUCCESS,
    FAILED
}
```

- [ ] **Step 6: Create Order entity**

```java
package com.auction.order.domain.entity;

import com.auction.order.domain.enums.OrderStatus;
import com.auction.order.domain.enums.OrderType;
import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("orders")
public class Order {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long userId;

    private OrderType type;

    private Long referenceId;

    private BigDecimal amount;

    private OrderStatus status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    private LocalDateTime paidAt;
}
```

- [ ] **Step 7: Create Payment entity**

```java
package com.auction.order.domain.entity;

import com.auction.order.domain.enums.PaymentStatus;
import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("payment")
public class Payment {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long orderId;

    private String paymentMethod;

    private BigDecimal amount;

    private PaymentStatus status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

- [ ] **Step 8: Create IdempotencyLog entity**

```java
package com.auction.order.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("idempotency_log")
public class IdempotencyLog {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String idempotencyKey;

    private String eventType;

    private LocalDateTime processedAt;
}
```

- [ ] **Step 9: Commit**

```bash
git add order-service/pom.xml order-service/src/main/resources/bootstrap.yml order-service/src/main/java/com/auction/order/domain/
git commit -m "feat(order): add pom dependencies, bootstrap config, and domain entities"
```

---

### Task 4: Create repository mappers and infrastructure configs

**Files:**
- Create: `order-service/src/main/java/com/auction/order/repository/OrderMapper.java`
- Create: `order-service/src/main/java/com/auction/order/repository/PaymentMapper.java`
- Create: `order-service/src/main/java/com/auction/order/repository/IdempotencyLogMapper.java`
- Create: `order-service/src/main/java/com/auction/order/config/MyBatisPlusConfig.java`
- Create: `order-service/src/main/java/com/auction/order/config/KafkaConfig.java`
- Create: `order-service/src/main/java/com/auction/order/config/RabbitMQConfig.java`
- Create: `order-service/src/main/java/com/auction/order/config/SeataConfig.java`
- Create: `order-service/src/main/java/com/auction/order/config/FeignConfig.java`
- Modify: `order-service/src/main/java/com/auction/order/OrderServiceApplication.java`
- Modify: `order-service/src/main/resources/application.yml`

- [ ] **Step 1: Create OrderMapper**

```java
package com.auction.order.repository;

import com.auction.order.domain.entity.Order;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

public interface OrderMapper extends BaseMapper<Order> {

    @Update("UPDATE orders SET status = #{status} WHERE id = #{id} AND status = #{expectedStatus}")
    int compareAndSetStatus(@Param("id") Long id,
                            @Param("expectedStatus") String expectedStatus,
                            @Param("status") String status);
}
```

- [ ] **Step 2: Create PaymentMapper**

```java
package com.auction.order.repository;

import com.auction.order.domain.entity.Payment;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;

public interface PaymentMapper extends BaseMapper<Payment> {
}
```

- [ ] **Step 3: Create IdempotencyLogMapper**

```java
package com.auction.order.repository;

import com.auction.order.domain.entity.IdempotencyLog;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface IdempotencyLogMapper extends BaseMapper<IdempotencyLog> {

    @Select("SELECT COUNT(*) FROM idempotency_log WHERE idempotency_key = #{key}")
    int existsByKey(@Param("key") String key);
}
```

- [ ] **Step 4: Create MyBatisPlusConfig**

```java
package com.auction.order.config;

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
@MapperScan("com.auction.order.repository")
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

- [ ] **Step 5: Create KafkaConfig**

```java
package com.auction.order.config;

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
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderEventsDeadLetterTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_EVENTS_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(
                        KafkaTopics.ORDER_EVENTS_DLT, record.partition()));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }
}
```

- [ ] **Step 6: Create RabbitMQConfig**

```java
package com.auction.order.config;

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
    public static final String ORDER_TIMEOUT_QUEUE = "order-timeout-queue";
    public static final String ORDER_TIMEOUT_ROUTING_KEY = "order.timeout";

    @Bean
    public CustomExchange delayExchange() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        return new CustomExchange(DELAY_EXCHANGE, "x-delayed-message", true, false, args);
    }

    @Bean
    public Queue orderTimeoutQueue() {
        return new Queue(ORDER_TIMEOUT_QUEUE, true);
    }

    @Bean
    public Binding orderTimeoutBinding() {
        return BindingBuilder.bind(orderTimeoutQueue())
                .to(delayExchange())
                .with(ORDER_TIMEOUT_ROUTING_KEY)
                .noargs();
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
```

- [ ] **Step 7: Create SeataConfig**

Seata AT mode requires a DataSource proxy so it can record before/after snapshots in `undo_log`. This config wraps the real DataSource with Seata's `DataSourceProxy`.

```java
package com.auction.order.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import io.seata.rm.datasource.DataSourceProxy;
import org.apache.ibatis.reflection.MetaObject;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class SeataConfig {

    @Bean
    @ConditionalOnMissingBean(DataSourceProxy.class)
    public DataSourceProxy dataSourceProxy(DataSource dataSource) {
        return new DataSourceProxy(dataSource);
    }
}
```

**Important:** Remove the `MyBatisPlusInterceptor` and `@MapperScan` from `MyBatisPlusConfig.java` (created in Step 4) and instead keep them there. Actually, keep `MyBatisPlusConfig` as-is. The `SeataConfig` simply wraps the existing DataSource with Seata's proxy. MyBatis-Plus will automatically use the proxied DataSource.

Wait -- actually there is a subtlety. The `SeataConfig` must create the `DataSourceProxy` bean, and Spring will use it instead of the auto-configured DataSource for MyBatis. This works because `DataSourceProxy` implements `DataSource`. However, we need to make sure the bean ordering is correct. The simplest approach: `SeataConfig` creates a `DataSourceProxy` from the auto-configured `DataSource`, and MyBatis-Plus picks it up automatically.

- [ ] **Step 8: Create FeignConfig**

```java
package com.auction.order.config;

import com.auction.common.feign.SeataFeignInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignConfig {

    @Bean
    public SeataFeignInterceptor seataFeignInterceptor() {
        return new SeataFeignInterceptor();
    }
}
```

- [ ] **Step 9: Update OrderServiceApplication with annotations**

Replace `order-service/src/main/java/com/auction/order/OrderServiceApplication.java` with:

```java
package com.auction.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "com.auction.order.client")
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
```

- [ ] **Step 10: Replace application.yml**

Replace `order-service/src/main/resources/application.yml` with:

```yaml
server:
  port: 8083

spring:
  cloud:
    nacos:
      discovery:
        server-addr: localhost:8848
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://localhost:3306/order_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true
    username: root
    password: root
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
      group-id: order-service
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "com.auction.common.event,com.auction.common.event.order,com.auction.common.event.ticket"
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

seata:
  enabled: true
  application-id: order-service
  tx-service-group: auction-tx-group
  service:
    vgroup-mapping:
      auction-tx-group: default
  registry:
    type: nacos
    nacos:
      server-addr: localhost:8848
      namespace:
      group: SEATA_GROUP
      application: seata-server

payment:
  mock:
    success-rate: 0.95
    min-latency-ms: 500
    max-latency-ms: 2000

logging:
  level:
    com.auction.order: INFO
```

- [ ] **Step 11: Commit**

```bash
git add order-service/
git commit -m "feat(order): add repository mappers, configs, and application setup"
```

---

### Task 5: Create DTOs, Feign client, and event producer

**Files:**
- Create: `order-service/src/main/java/com/auction/order/controller/dto/CreateOrderRequest.java`
- Create: `order-service/src/main/java/com/auction/order/controller/dto/OrderResponse.java`
- Create: `order-service/src/main/java/com/auction/order/controller/dto/PayResponse.java`
- Create: `order-service/src/main/java/com/auction/order/client/TicketFeignClient.java`
- Create: `order-service/src/main/java/com/auction/order/event/OrderEventProducer.java`

- [ ] **Step 1: Create CreateOrderRequest DTO**

```java
package com.auction.order.controller.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class CreateOrderRequest {

    @NotNull(message = "reservationId is required")
    private Long reservationId;

    @NotNull(message = "amount is required")
    @Positive(message = "amount must be positive")
    private BigDecimal amount;
}
```

- [ ] **Step 2: Create OrderResponse DTO**

```java
package com.auction.order.controller.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
public class OrderResponse {

    private Long id;
    private Long userId;
    private String type;
    private Long referenceId;
    private BigDecimal amount;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime paidAt;
}
```

- [ ] **Step 3: Create PayResponse DTO**

```java
package com.auction.order.controller.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PayResponse {

    private Long orderId;
    private String status;
    private String paymentStatus;
    private String message;
}
```

- [ ] **Step 4: Create TicketFeignClient**

order-service calls ticket-service to confirm reservation after payment succeeds, and to release stock on order cancellation/timeout.

```java
package com.auction.order.client;

import com.auction.common.dto.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

@FeignClient(name = "ticket-service", path = "/api/tickets")
public interface TicketFeignClient {

    @PostMapping("/{reservationId}/confirm")
    ApiResponse<Void> confirmReservation(@PathVariable("reservationId") Long reservationId);

    @DeleteMapping("/{reservationId}")
    ApiResponse<Void> cancelReservation(@PathVariable("reservationId") Long reservationId);
}
```

- [ ] **Step 5: Create OrderEventProducer**

```java
package com.auction.order.event;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.order.OrderCancelledEvent;
import com.auction.common.event.order.OrderCreatedEvent;
import com.auction.common.event.order.OrderExpiredEvent;
import com.auction.common.event.order.PaymentCompletedEvent;
import com.auction.common.event.order.PaymentInitiatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(Long orderId, Long userId, String orderType,
                                    Long referenceId, BigDecimal amount) {
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, userId, orderType, referenceId, amount);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishPaymentInitiated(Long orderId, Long userId, BigDecimal amount) {
        PaymentInitiatedEvent event = new PaymentInitiatedEvent(orderId, userId, amount);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishPaymentCompleted(Long orderId, Long userId, BigDecimal amount) {
        PaymentCompletedEvent event = new PaymentCompletedEvent(orderId, userId, amount);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishOrderCancelled(Long orderId, Long userId, String reason) {
        OrderCancelledEvent event = new OrderCancelledEvent(orderId, userId, reason);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishOrderExpired(Long orderId, Long userId) {
        OrderExpiredEvent event = new OrderExpiredEvent(orderId, userId);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }
}
```

- [ ] **Step 6: Commit**

```bash
git add order-service/src/main/java/com/auction/order/controller/dto/ order-service/src/main/java/com/auction/order/client/ order-service/src/main/java/com/auction/order/event/
git commit -m "feat(order): add DTOs, TicketFeignClient, and OrderEventProducer"
```

---

### Task 6: Implement OrderService with state machine and mock payment

**Files:**
- Create: `order-service/src/main/java/com/auction/order/service/OrderService.java`
- Create: `order-service/src/main/java/com/auction/order/service/impl/OrderServiceImpl.java`

This is the core service. It implements the order state machine, mock payment logic, and handles order creation from both ticket purchases and auction settlements.

- [ ] **Step 1: Create OrderService interface**

```java
package com.auction.order.service;

import com.auction.order.controller.dto.CreateOrderRequest;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.controller.dto.PayResponse;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

public interface OrderService {

    OrderResponse createFromTicket(Long userId, CreateOrderRequest request);

    OrderResponse createFromAuction(Long auctionId, Long winnerId,
                                    java.math.BigDecimal amount);

    PayResponse pay(Long userId, Long orderId);

    void cancel(Long userId, Long orderId);

    OrderResponse getById(Long userId, Long orderId);

    Page<OrderResponse> listByUserId(Long userId, int page, int size);
}
```

- [ ] **Step 2: Create OrderServiceImpl**

```java
package com.auction.order.service.impl;

import com.auction.common.exception.BusinessException;
import com.auction.order.client.TicketFeignClient;
import com.auction.order.config.RabbitMQConfig;
import com.auction.order.controller.dto.CreateOrderRequest;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.controller.dto.PayResponse;
import com.auction.order.domain.enums.OrderStatus;
import com.auction.order.domain.enums.OrderType;
import com.auction.order.domain.enums.PaymentStatus;
import com.auction.order.domain.entity.Order;
import com.auction.order.domain.entity.Payment;
import com.auction.order.event.OrderEventProducer;
import com.auction.order.repository.OrderMapper;
import com.auction.order.repository.PaymentMapper;
import com.auction.order.service.OrderService;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;

@Service
public class OrderServiceImpl implements OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceImpl.class);
    private static final int ORDER_TIMEOUT_MINUTES = 30;
    private static final Random RANDOM = new Random();

    private final OrderMapper orderMapper;
    private final PaymentMapper paymentMapper;
    private final TicketFeignClient ticketFeignClient;
    private final OrderEventProducer eventProducer;
    private final RabbitTemplate rabbitTemplate;

    @Value("${payment.mock.success-rate:0.95}")
    private double successRate;

    @Value("${payment.mock.min-latency-ms:500}")
    private int minLatencyMs;

    @Value("${payment.mock.max-latency-ms:2000}")
    private int maxLatencyMs;

    public OrderServiceImpl(OrderMapper orderMapper,
                            PaymentMapper paymentMapper,
                            TicketFeignClient ticketFeignClient,
                            OrderEventProducer eventProducer,
                            RabbitTemplate rabbitTemplate) {
        this.orderMapper = orderMapper;
        this.paymentMapper = paymentMapper;
        this.ticketFeignClient = ticketFeignClient;
        this.eventProducer = eventProducer;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    @Transactional
    public OrderResponse createFromTicket(Long userId, CreateOrderRequest request) {
        Order order = new Order();
        order.setUserId(userId);
        order.setType(OrderType.TICKET);
        order.setReferenceId(request.getReservationId());
        order.setAmount(request.getAmount());
        order.setStatus(OrderStatus.CREATED);
        orderMapper.insert(order);

        final Long orderId = order.getId();
        sendTimeoutMessage(orderId);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishOrderCreated(orderId, userId, "TICKET",
                        request.getReservationId(), request.getAmount());
                log.info("Created TICKET order: id={}, userId={}, reservationId={}",
                        orderId, userId, request.getReservationId());
            }
        });

        return toResponse(order);
    }

    @Override
    @Transactional
    public OrderResponse createFromAuction(Long auctionId, Long winnerId, BigDecimal amount) {
        Order order = new Order();
        order.setUserId(winnerId);
        order.setType(OrderType.AUCTION);
        order.setReferenceId(auctionId);
        order.setAmount(amount);
        order.setStatus(OrderStatus.CREATED);
        orderMapper.insert(order);

        final Long orderId = order.getId();
        sendTimeoutMessage(orderId);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishOrderCreated(orderId, winnerId, "AUCTION",
                        auctionId, amount);
                log.info("Created AUCTION order: id={}, winnerId={}, auctionId={}",
                        orderId, winnerId, auctionId);
            }
        });

        return toResponse(order);
    }

    @Override
    @Transactional
    public PayResponse pay(Long userId, Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BusinessException(404, "Order not found");
        }
        if (!order.getUserId().equals(userId)) {
            throw new BusinessException(403, "Not your order");
        }

        // Idempotent: if already PAYING or PAID, return current state
        if (order.getStatus() == OrderStatus.PAYING) {
            return PayResponse.builder()
                    .orderId(orderId)
                    .status(OrderStatus.PAYING.name())
                    .paymentStatus("IN_PROGRESS")
                    .message("Payment already in progress")
                    .build();
        }
        if (order.getStatus() == OrderStatus.PAID) {
            return PayResponse.builder()
                    .orderId(orderId)
                    .status(OrderStatus.PAID.name())
                    .paymentStatus("SUCCESS")
                    .message("Already paid")
                    .build();
        }
        if (order.getStatus() == OrderStatus.COMPLETED) {
            return PayResponse.builder()
                    .orderId(orderId)
                    .status(OrderStatus.COMPLETED.name())
                    .paymentStatus("SUCCESS")
                    .message("Order already completed")
                    .build();
        }

        // State check: only CREATED can transition to PAYING
        if (order.getStatus() != OrderStatus.CREATED) {
            throw new BusinessException(409, "Cannot pay order in status " + order.getStatus());
        }

        // CREATED -> PAYING
        int rows = orderMapper.compareAndSetStatus(orderId,
                OrderStatus.CREATED.name(), OrderStatus.PAYING.name());
        if (rows == 0) {
            throw new BusinessException(409, "Order status changed, please retry");
        }
        order.setStatus(OrderStatus.PAYING);

        Payment payment = new Payment();
        payment.setOrderId(orderId);
        payment.setPaymentMethod("MOCK");
        payment.setAmount(order.getAmount());
        payment.setStatus(PaymentStatus.PENDING);
        paymentMapper.insert(payment);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishPaymentInitiated(orderId, userId, order.getAmount());
            }
        });

        // Mock payment: synchronous simulation
        boolean success = simulatePayment();

        if (success) {
            return handlePaymentSuccess(orderId, userId, order.getAmount(), payment);
        } else {
            return handlePaymentFailure(orderId, payment);
        }
    }

    @Override
    @Transactional
    public void cancel(Long userId, Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BusinessException(404, "Order not found");
        }
        if (!order.getUserId().equals(userId)) {
            throw new BusinessException(403, "Not your order");
        }
        if (order.getStatus() != OrderStatus.CREATED) {
            throw new BusinessException(409, "Can only cancel orders in CREATED status");
        }

        int rows = orderMapper.compareAndSetStatus(orderId,
                OrderStatus.CREATED.name(), OrderStatus.CANCELLED.name());
        if (rows == 0) {
            throw new BusinessException(409, "Order status changed, please retry");
        }

        // Release ticket stock
        if (order.getType() == OrderType.TICKET) {
            try {
                ticketFeignClient.cancelReservation(order.getReferenceId());
            } catch (Exception e) {
                log.error("Failed to cancel reservation for order={}", orderId, e);
            }
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishOrderCancelled(orderId, userId, "USER_CANCEL");
                log.info("Cancelled order: id={}, userId={}", orderId, userId);
            }
        });
    }

    @Override
    public OrderResponse getById(Long userId, Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BusinessException(404, "Order not found");
        }
        if (!order.getUserId().equals(userId)) {
            throw new BusinessException(403, "Not your order");
        }
        return toResponse(order);
    }

    @Override
    public Page<OrderResponse> listByUserId(Long userId, int page, int size) {
        LambdaQueryWrapper<Order> wrapper = new LambdaQueryWrapper<Order>()
                .eq(Order::getUserId, userId)
                .orderByDesc(Order::getCreatedAt);

        Page<Order> orderPage = orderMapper.selectPage(new Page<>(page, size), wrapper);

        Page<OrderResponse> responsePage = new Page<>(orderPage.getCurrent(),
                orderPage.getSize(), orderPage.getTotal());
        responsePage.setRecords(orderPage.getRecords().stream()
                .map(this::toResponse)
                .toList());
        return responsePage;
    }

    /**
     * Called by OrderTimeoutConsumer when 30-min TTL fires.
     * Not @Transactional here -- the consumer manages its own TX.
     */
    public void expireOrder(Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            log.warn("Order not found for expiry: id={}", orderId);
            return;
        }
        if (order.getStatus() != OrderStatus.CREATED && order.getStatus() != OrderStatus.PAYING) {
            log.info("Order already processed: id={}, status={}", orderId, order.getStatus());
            return;
        }

        String expectedStatus = order.getStatus().name();
        int rows = orderMapper.compareAndSetStatus(orderId, expectedStatus,
                OrderStatus.EXPIRED.name());
        if (rows == 0) {
            log.info("Order status race: id={}", orderId);
            return;
        }

        // Release ticket stock
        if (order.getType() == OrderType.TICKET) {
            try {
                ticketFeignClient.cancelReservation(order.getReferenceId());
            } catch (Exception e) {
                log.error("Failed to cancel reservation for expired order={}", orderId, e);
            }
        }

        eventProducer.publishOrderExpired(orderId, order.getUserId());
        log.info("Expired order: id={}", orderId);
    }

    private boolean simulatePayment() {
        int latency = minLatencyMs + RANDOM.nextInt(maxLatencyMs - minLatencyMs);
        try {
            Thread.sleep(latency);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return RANDOM.nextDouble() < successRate;
    }

    @Transactional
    private PayResponse handlePaymentSuccess(Long orderId, Long userId,
                                              BigDecimal amount, Payment payment) {
        // PAYING -> PAID
        orderMapper.compareAndSetStatus(orderId,
                OrderStatus.PAYING.name(), OrderStatus.PAID.name());

        payment.setStatus(PaymentStatus.SUCCESS);
        paymentMapper.updateById(payment);

        // Update paid_at
        Order paidOrder = new Order();
        paidOrder.setId(orderId);
        paidOrder.setPaidAt(LocalDateTime.now());
        orderMapper.updateById(paidOrder);

        // Confirm ticket reservation
        Order order = orderMapper.selectById(orderId);
        if (order.getType() == OrderType.TICKET) {
            try {
                ticketFeignClient.confirmReservation(order.getReferenceId());
            } catch (Exception e) {
                log.error("Failed to confirm reservation for order={}", orderId, e);
            }
        }

        // PAID -> COMPLETED
        orderMapper.compareAndSetStatus(orderId,
                OrderStatus.PAID.name(), OrderStatus.COMPLETED.name());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishPaymentCompleted(orderId, userId, amount);
                log.info("Payment completed: orderId={}", orderId);
            }
        });

        return PayResponse.builder()
                .orderId(orderId)
                .status(OrderStatus.COMPLETED.name())
                .paymentStatus("SUCCESS")
                .message("Payment successful")
                .build();
    }

    @Transactional
    private PayResponse handlePaymentFailure(Long orderId, Payment payment) {
        // PAYING -> CREATED (user can retry)
        orderMapper.compareAndSetStatus(orderId,
                OrderStatus.PAYING.name(), OrderStatus.CREATED.name());

        payment.setStatus(PaymentStatus.FAILED);
        paymentMapper.updateById(payment);

        log.info("Payment failed (mock): orderId={}", orderId);

        return PayResponse.builder()
                .orderId(orderId)
                .status(OrderStatus.CREATED.name())
                .paymentStatus("FAILED")
                .message("Payment failed, please retry")
                .build();
    }

    private void sendTimeoutMessage(Long orderId) {
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.DELAY_EXCHANGE,
                RabbitMQConfig.ORDER_TIMEOUT_ROUTING_KEY,
                orderId,
                msg -> {
                    msg.getMessageProperties().setDelay(ORDER_TIMEOUT_MINUTES * 60 * 1000);
                    return msg;
                });
    }

    private OrderResponse toResponse(Order order) {
        return OrderResponse.builder()
                .id(order.getId())
                .userId(order.getUserId())
                .type(order.getType().name())
                .referenceId(order.getReferenceId())
                .amount(order.getAmount())
                .status(order.getStatus().name())
                .createdAt(order.getCreatedAt())
                .paidAt(order.getPaidAt())
                .build();
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add order-service/src/main/java/com/auction/order/service/
git commit -m "feat(order): implement OrderService with state machine and mock payment"
```

---

### Task 7: Implement controllers and timeout consumer

**Files:**
- Create: `order-service/src/main/java/com/auction/order/controller/OrderController.java`
- Create: `order-service/src/main/java/com/auction/order/controller/InternalOrderController.java`
- Create: `order-service/src/main/java/com/auction/order/consumer/OrderTimeoutConsumer.java`

- [ ] **Step 1: Create OrderController (public endpoints)**

```java
package com.auction.order.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.common.security.UserContextHolder;
import com.auction.order.controller.dto.CreateOrderRequest;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.controller.dto.PayResponse;
import com.auction.order.service.OrderService;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<ApiResponse<OrderResponse>> createOrder(
            @Valid @RequestBody CreateOrderRequest request) {
        UserContext ctx = UserContextHolder.get();
        OrderResponse response = orderService.createFromTicket(ctx.getUserId(), request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<OrderResponse>> getOrder(@PathVariable Long id) {
        UserContext ctx = UserContextHolder.get();
        OrderResponse response = orderService.getById(ctx.getUserId(), id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @GetMapping
    public ResponseEntity<ApiResponse<Page<OrderResponse>>> listOrders(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size) {
        UserContext ctx = UserContextHolder.get();
        Page<OrderResponse> result = orderService.listByUserId(ctx.getUserId(), page, size);
        return ResponseEntity.ok(ApiResponse.success(result));
    }

    @PostMapping("/{id}/pay")
    public ResponseEntity<ApiResponse<PayResponse>> pay(@PathVariable Long id) {
        UserContext ctx = UserContextHolder.get();
        PayResponse response = orderService.pay(ctx.getUserId(), id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<ApiResponse<Void>> cancel(@PathVariable Long id) {
        UserContext ctx = UserContextHolder.get();
        orderService.cancel(ctx.getUserId(), id);
        return ResponseEntity.ok(ApiResponse.success(null));
    }
}
```

- [ ] **Step 2: Create InternalOrderController (for Seata auction settlement)**

This endpoint is called by auction-service via Feign within a Seata global transaction. It is NOT routed through the gateway (no `/api/orders/internal` route in gateway config).

```java
package com.auction.order.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/orders/internal")
public class InternalOrderController {

    private final OrderService orderService;

    public InternalOrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/auction")
    public ResponseEntity<ApiResponse<OrderResponse>> createAuctionOrder(
            @RequestBody Map<String, Object> body) {
        Long auctionId = Long.parseLong(body.get("auctionId").toString());
        Long winnerId = Long.parseLong(body.get("winnerId").toString());
        BigDecimal amount = new BigDecimal(body.get("amount").toString());

        OrderResponse response = orderService.createFromAuction(auctionId, winnerId, amount);
        return ResponseEntity.ok(ApiResponse.success(response));
    }
}
```

- [ ] **Step 3: Create OrderTimeoutConsumer**

```java
package com.auction.order.consumer;

import com.auction.order.service.impl.OrderServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderTimeoutConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutConsumer.class);

    private final OrderServiceImpl orderService;

    public OrderTimeoutConsumer(OrderServiceImpl orderService) {
        this.orderService = orderService;
    }

    @RabbitListener(queues = "order-timeout-queue")
    @Transactional
    public void handleOrderTimeout(Long orderId) {
        log.info("Received order timeout delayed message for orderId={}", orderId);
        orderService.expireOrder(orderId);
    }
}
```

- [ ] **Step 4: Commit**

```bash
git add order-service/src/main/java/com/auction/order/controller/ order-service/src/main/java/com/auction/order/consumer/
git commit -m "feat(order): add public/internal controllers and order timeout consumer"
```

---

### Task 8: Add Seata + Feign to auction-service

**Files:**
- Modify: `auction-service/pom.xml`
- Modify: `auction-service/src/main/resources/application.yml`
- Create: `auction-service/src/main/java/com/auction/auction/config/SeataConfig.java`
- Create: `auction-service/src/main/java/com/auction/auction/config/FeignConfig.java`
- Create: `auction-service/src/main/java/com/auction/auction/client/TicketFeignClient.java`
- Create: `auction-service/src/main/java/com/auction/auction/client/OrderFeignClient.java`
- Create: `auction-service/src/main/java/com/auction/auction/service/SettlementService.java`
- Modify: `auction-service/src/main/java/com/auction/auction/scheduler/AuctionLifecycleScheduler.java`
- Modify: `auction-service/src/main/java/com/auction/auction/AuctionServiceApplication.java`

- [ ] **Step 1: Add deps to auction-service pom.xml**

Add these dependencies to `auction-service/pom.xml` (after the existing `<dependencies>` entries, before `</dependencies>`):

```xml
<!-- OpenFeign -->
<dependency>
    <groupId>org.springframework.cloud</groupId>
    <artifactId>spring-cloud-starter-openfeign</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.cloud</groupId>
    <artifactId>spring-cloud-starter-loadbalancer</artifactId>
</dependency>

<!-- Seata -->
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-starter-alibaba-seata</artifactId>
    <exclusions>
        <exclusion>
            <groupId>io.seata</groupId>
            <artifactId>seata-spring-boot-starter</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>io.seata</groupId>
    <artifactId>seata-spring-boot-starter</artifactId>
    <version>${seata.version}</version>
</dependency>
```

- [ ] **Step 2: Add Seata config to auction-service application.yml**

Append these lines to `auction-service/src/main/resources/application.yml`:

```yaml
seata:
  enabled: true
  application-id: auction-service
  tx-service-group: auction-tx-group
  service:
    vgroup-mapping:
      auction-tx-group: default
  registry:
    type: nacos
    nacos:
      server-addr: localhost:8848
      namespace:
      group: SEATA_GROUP
      application: seata-server
```

- [ ] **Step 3: Create SeataConfig for auction-service**

```java
package com.auction.auction.config;

import io.seata.rm.datasource.DataSourceProxy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class SeataConfig {

    @Bean
    @ConditionalOnMissingBean(DataSourceProxy.class)
    public DataSourceProxy dataSourceProxy(DataSource dataSource) {
        return new DataSourceProxy(dataSource);
    }
}
```

- [ ] **Step 4: Create FeignConfig for auction-service**

```java
package com.auction.auction.config;

import com.auction.common.feign.SeataFeignInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignConfig {

    @Bean
    public SeataFeignInterceptor seataFeignInterceptor() {
        return new SeataFeignInterceptor();
    }
}
```

- [ ] **Step 5: Create TicketFeignClient for auction-service**

This calls the internal `settle-reserve` endpoint on ticket-service, which is MySQL-only (no Redis) for Seata compatibility.

```java
package com.auction.auction.client;

import com.auction.common.dto.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.Map;

@FeignClient(name = "ticket-service", path = "/api/tickets")
public interface TicketFeignClient {

    @PostMapping("/internal/settle-reserve")
    ApiResponse<Map<String, Object>> settleReserve(@RequestBody Map<String, Object> body);
}
```

- [ ] **Step 6: Create OrderFeignClient for auction-service**

```java
package com.auction.auction.client;

import com.auction.common.dto.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.Map;

@FeignClient(name = "order-service", path = "/api/orders")
public interface OrderFeignClient {

    @PostMapping("/internal/auction")
    ApiResponse<Map<String, Object>> createAuctionOrder(@RequestBody Map<String, Object> body);
}
```

- [ ] **Step 7: Update AuctionServiceApplication with @EnableFeignClients**

Read the current `AuctionServiceApplication.java` and add the annotation:

```java
package com.auction.auction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "com.auction.auction.client")
public class AuctionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuctionServiceApplication.class, args);
    }
}
```

- [ ] **Step 8: Create SettlementService**

This service encapsulates the Seata-distributed transaction for auction settlement.

```java
package com.auction.auction.service;

import com.auction.auction.client.OrderFeignClient;
import com.auction.auction.client.TicketFeignClient;
import com.auction.common.dto.ApiResponse;
import io.seata.spring.annotation.GlobalTransactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Service
public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    private final TicketFeignClient ticketFeignClient;
    private final OrderFeignClient orderFeignClient;

    public SettlementService(TicketFeignClient ticketFeignClient,
                             OrderFeignClient orderFeignClient) {
        this.ticketFeignClient = ticketFeignClient;
        this.orderFeignClient = orderFeignClient;
    }

    @GlobalTransactional(name = "auction-settlement", rollbackFor = Exception.class)
    public void settle(Long auctionId, Long winnerId, BigDecimal amount, Long ticketTypeId) {
        log.info("Starting Seata settlement TX: auction={}, winner={}, amount={}",
                auctionId, winnerId, amount);

        // Step 1: Reserve stock in ticket-service (MySQL-only, Seata RM)
        Map<String, Object> reserveBody = new HashMap<>();
        reserveBody.put("auctionId", auctionId);
        reserveBody.put("winnerId", winnerId);
        reserveBody.put("ticketTypeId", ticketTypeId);
        reserveBody.put("quantity", 1);
        ApiResponse<Map<String, Object>> reserveResult =
                ticketFeignClient.settleReserve(reserveBody);
        if (reserveResult == null || reserveResult.getCode() != 200) {
            throw new RuntimeException("Stock reservation failed: " +
                    (reserveResult != null ? reserveResult.getMessage() : "null response"));
        }

        // Step 2: Create order in order-service (Seata RM)
        Map<String, Object> orderBody = new HashMap<>();
        orderBody.put("auctionId", auctionId);
        orderBody.put("winnerId", winnerId);
        orderBody.put("amount", amount);
        ApiResponse<Map<String, Object>> orderResult =
                orderFeignClient.createAuctionOrder(orderBody);
        if (orderResult == null || orderResult.getCode() != 200) {
            throw new RuntimeException("Order creation failed: " +
                    (orderResult != null ? orderResult.getMessage() : "null response"));
        }

        log.info("Seata settlement TX completed: auction={}", auctionId);
    }
}
```

- [ ] **Step 9: Modify AuctionLifecycleScheduler to use SettlementService**

The scheduler's `expireActive()` method currently publishes an `AuctionSettledEvent` to Kafka for settlement. We need to change it to call `SettlementService.settle()` synchronously within a Seata TX when there is a winning bid, and still publish `AuctionSettledEvent` after success.

Read the current `AuctionLifecycleScheduler.java`. Modify the `expireActive()` method:

Replace the section inside `if (highest != null) { ... }` block. The current code:

```java
if (highest != null) {
    int rows = auctionMapper.markSettled(
            auction.getId(), highest.getUserId(), highest.getAmount());
    if (rows == 0) {
        continue;
    }
    AuctionSettledEvent evt = new AuctionSettledEvent(
            auction.getId(), highest.getUserId(),
            highest.getAmount(), auction.getTicketTypeId());
    publish(auction.getId(), evt);
    log.info("Auction {} settled: winner={} amount={}",
            auction.getId(), highest.getUserId(), highest.getAmount());
}
```

Becomes:

```java
if (highest != null) {
    int rows = auctionMapper.markSettled(
            auction.getId(), highest.getUserId(), highest.getAmount());
    if (rows == 0) {
        continue;
    }
    try {
        settlementService.settle(
                auction.getId(), highest.getUserId(),
                highest.getAmount(), auction.getTicketTypeId());
    } catch (Exception e) {
        log.error("Seata settlement failed for auction={}, rolling back auction status",
                auction.getId(), e);
        // Revert auction status so it can be retried on next poll
        auctionMapper.markActive(auction.getId());
        redis.opsForValue().set(RedisKeys.auctionStatus(auction.getId()), "ACTIVE");
        continue;
    }
    AuctionSettledEvent evt = new AuctionSettledEvent(
            auction.getId(), highest.getUserId(),
            highest.getAmount(), auction.getTicketTypeId());
    publish(auction.getId(), evt);
    log.info("Auction {} settled: winner={} amount={}",
            auction.getId(), highest.getUserId(), highest.getAmount());
}
```

Also add `SettlementService` as a constructor dependency. Add this field and constructor parameter:

```java
private final SettlementService settlementService;
```

Update the constructor to include it.

- [ ] **Step 10: Commit**

```bash
git add auction-service/
git commit -m "feat(auction): add Seata AT distributed transaction for auction settlement"
```

---

### Task 9: Add internal settle-reserve endpoint to ticket-service

**Files:**
- Modify: `ticket-service/pom.xml`
- Modify: `ticket-service/src/main/resources/application.yml`
- Create: `ticket-service/src/main/java/com/auction/ticket/config/SeataConfig.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/config/FeignConfig.java`
- Create: `ticket-service/src/main/java/com/auction/ticket/controller/InternalTicketController.java`
- Modify: `ticket-service/src/main/java/com/auction/ticket/service/TicketStockService.java`
- Modify: `ticket-service/src/main/java/com/auction/ticket/service/impl/TicketStockServiceImpl.java`

- [ ] **Step 1: Add deps to ticket-service pom.xml**

Add these dependencies to `ticket-service/pom.xml` (before `</dependencies>`):

```xml
<!-- OpenFeign -->
<dependency>
    <groupId>org.springframework.cloud</groupId>
    <artifactId>spring-cloud-starter-openfeign</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.cloud</groupId>
    <artifactId>spring-cloud-starter-loadbalancer</artifactId>
</dependency>

<!-- Seata -->
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-starter-alibaba-seata</artifactId>
    <exclusions>
        <exclusion>
            <groupId>io.seata</groupId>
            <artifactId>seata-spring-boot-starter</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>io.seata</groupId>
    <artifactId>seata-spring-boot-starter</artifactId>
    <version>${seata.version}</version>
</dependency>
```

- [ ] **Step 2: Add Seata config to ticket-service application.yml**

Append these lines to `ticket-service/src/main/resources/application.yml`:

```yaml
seata:
  enabled: true
  application-id: ticket-service
  tx-service-group: auction-tx-group
  service:
    vgroup-mapping:
      auction-tx-group: default
  registry:
    type: nacos
    nacos:
      server-addr: localhost:8848
      namespace:
      group: SEATA_GROUP
      application: seata-server
```

- [ ] **Step 3: Create SeataConfig for ticket-service**

```java
package com.auction.ticket.config;

import io.seata.rm.datasource.DataSourceProxy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class SeataConfig {

    @Bean
    @ConditionalOnMissingBean(DataSourceProxy.class)
    public DataSourceProxy dataSourceProxy(DataSource dataSource) {
        return new DataSourceProxy(dataSource);
    }
}
```

- [ ] **Step 4: Create FeignConfig for ticket-service**

```java
package com.auction.ticket.config;

import com.auction.common.feign.SeataFeignInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignConfig {

    @Bean
    public SeataFeignInterceptor seataFeignInterceptor() {
        return new SeataFeignInterceptor();
    }
}
```

- [ ] **Step 5: Add settleReserve to TicketStockService interface**

Add this method to the existing `TicketStockService` interface:

```java
Long settleReserve(Long ticketTypeId, Long userId, int quantity);
```

- [ ] **Step 6: Implement settleReserve in TicketStockServiceImpl**

This is a MySQL-only operation -- no Redis involvement. Seata undo_log can fully reverse the MySQL changes if the global TX rolls back.

Add this method to `TicketStockServiceImpl`:

```java
@Override
@Transactional
public Long settleReserve(Long ticketTypeId, Long userId, int quantity) {
    // ticketTypeId maps to ticket_stock.id in this settlement flow
    TicketStock stock = stockMapper.selectById(ticketTypeId);
    if (stock == null) {
        throw new BusinessException(404, "Ticket stock not found");
    }

    int available = stock.getTotalQuantity() - stock.getReservedQuantity() - stock.getSoldQuantity();
    if (available < quantity) {
        throw new BusinessException(400, "Out of stock");
    }

    Reservation reservation = new Reservation();
    reservation.setStockId(stock.getId());
    reservation.setUserId(userId);
    reservation.setQuantity(quantity);
    reservation.setStatus(ReservationStatus.PENDING);
    reservation.setExpireAt(LocalDateTime.now().plusMinutes(RESERVATION_TIMEOUT_MINUTES));
    reservationMapper.insert(reservation);

    stockMapper.incrementReserved(stock.getId(), quantity);

    log.info("Settle-reserved {} tickets for stockId={}, userId={}, reservationId={}",
            quantity, stock.getId(), userId, reservation.getId());

    return reservation.getId();
}
```

Also add `import java.time.LocalDateTime;` if not already present.

- [ ] **Step 7: Create InternalTicketController**

```java
package com.auction.ticket.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.ticket.service.TicketStockService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/tickets/internal")
public class InternalTicketController {

    private final TicketStockService ticketStockService;

    public InternalTicketController(TicketStockService ticketStockService) {
        this.ticketStockService = ticketStockService;
    }

    @PostMapping("/settle-reserve")
    public ResponseEntity<ApiResponse<Map<String, Object>>> settleReserve(
            @RequestBody Map<String, Object> body) {
        Long ticketTypeId = Long.parseLong(body.get("ticketTypeId").toString());
        Long winnerId = Long.parseLong(body.get("winnerId").toString());
        int quantity = Integer.parseInt(body.get("quantity").toString());

        Long reservationId = ticketStockService.settleReserve(ticketTypeId, winnerId, quantity);

        Map<String, Object> result = new HashMap<>();
        result.put("reservationId", reservationId);
        return ResponseEntity.ok(ApiResponse.success(result));
    }
}
```

- [ ] **Step 8: Update TicketServiceApplication with @EnableFeignClients**

Read `ticket-service/src/main/java/com/auction/ticket/TicketServiceApplication.java` and add:

```java
@EnableFeignClients(basePackages = "com.auction.ticket")
```

The full file becomes:

```java
package com.auction.ticket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "com.auction.ticket")
public class TicketServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TicketServiceApplication.class, args);
    }
}
```

- [ ] **Step 9: Commit**

```bash
git add ticket-service/
git commit -m "feat(ticket): add Seata integration and internal settle-reserve endpoint"
```

---

### Task 10: Build verification and fix any compilation errors

- [ ] **Step 1: Run Maven build for auction-common**

```bash
cd D:/codeproject/高并发分布式事件驱动系统 && mvn clean compile -pl auction-common
```

Expected: BUILD SUCCESS

- [ ] **Step 2: Run Maven build for all modified services**

```bash
cd D:/codeproject/高并发分布式事件驱动系统 && mvn clean compile -pl auction-common,order-service,auction-service,ticket-service -am
```

Expected: BUILD SUCCESS

- [ ] **Step 3: Fix any compilation errors**

Read the error output, fix the issues, and re-run the build.

- [ ] **Step 4: Run full project build**

```bash
cd D:/codeproject/高并发分布式事件驱动系统 && mvn clean compile
```

Expected: BUILD SUCCESS across all modules.

- [ ] **Step 5: Commit any fixes**

If build fixes were needed, commit them:

```bash
git add -A
git commit -m "fix: resolve compilation errors from P4 integration"
```

---

## Self-Review Checklist

- [x] **Spec coverage:**
  - US-001 (Create order from ticket): Task 5 `CreateOrderRequest` + Task 6 `createFromTicket()` + Task 7 `OrderController.createOrder()` -> covered
  - US-002 (Create order from auction): Task 6 `createFromAuction()` + Task 7 `InternalOrderController` -> covered
  - US-003 (Order state machine): Task 6 state transitions in `pay()`, `cancel()`, `expireOrder()` -> covered
  - US-004 (Mock payment): Task 6 `simulatePayment()` + configurable rate/latency -> covered
  - US-005 (Seata distributed TX): Task 2 `SeataFeignInterceptor` + Task 8 `SettlementService` + Task 9 `settleReserve` -> covered
  - US-006 (Order timeout): Task 7 `OrderTimeoutConsumer` + Task 6 `expireOrder()` + Task 4 `RabbitMQConfig` -> covered
  - US-007 (Query orders): Task 6 `getById()` + `listByUserId()` + Task 7 `OrderController` -> covered

- [x] **Placeholder scan:** No TBD, TODO, or placeholder steps found. All code blocks contain complete implementations.

- [x] **Type consistency:**
  - `OrderResponse.builder()` fields match `Order` entity fields across Task 6 `toResponse()` and Task 5 DTO
  - `PayResponse.builder()` fields consistent between Task 5 DTO and Task 6 `handlePaymentSuccess/Failure`
  - Feign client method signatures match controller endpoint signatures (Task 5 `TicketFeignClient`, Task 8 `TicketFeignClient`/`OrderFeignClient`)
  - `SettlementService.settle()` parameters match what `AuctionLifecycleScheduler` passes
  - `compareAndSetStatus()` mapper method used consistently in `OrderServiceImpl`, `TicketStockServiceImpl`

- [x] **Undo_log tables:** Already exist in all 3 databases (`auction_db`, `ticket_db`, `order_db`) from SQL init scripts
- [x] **Seata Server:** Already configured in docker-compose with Nacos registry
- [x] **Gateway routing:** `/api/orders/**` route already exists in gateway config
- [x] **Internal endpoints not exposed via gateway:** `/api/orders/internal/**` and `/api/tickets/internal/**` are not in gateway predicates, so they are only accessible within the service mesh
