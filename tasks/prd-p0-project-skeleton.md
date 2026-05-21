# PRD: P0 - Project Skeleton & Infrastructure

## Introduction

搭建项目的骨架结构：Maven 多模块父工程、Docker Compose 编排所有基础设施、Nacos 服务注册与配置中心、Spring Cloud Gateway 基础路由、MySQL 数据库初始化脚本、Seata Server 部署。这是所有后续阶段的基础。

## Goals

- 建立 Maven 多模块项目结构，所有微服务共享父 POM 版本管理
- Docker Compose 一键启动所有基础设施（MySQL、Redis、MongoDB、Kafka、RabbitMQ、Nacos、Seata、Zipkin、ELK）
- Gateway-service 能启动并转发请求到下游服务（下游服务暂时返回空响应）
- MySQL 初始化脚本创建所有服务的数据库和表结构
- Nacos 作为服务注册中心和配置中心可用
- Seata Server 启动并可接受事务注册

## User Stories

### US-001: Create Maven multi-module parent POM
**Description:** As a developer, I need a parent POM that manages all dependency versions so that microservices stay consistent.

**Acceptance Criteria:**
- [ ] Parent POM defines Spring Boot 3.x, Spring Cloud Alibaba, and all shared dependency versions
- [ ] Each microservice is a `<module>` in parent POM
- [ ] `mvn clean compile` succeeds from project root
- [ ] Shared module `auction-common` exists for common DTOs and event classes

### US-002: Write Docker Compose with all infrastructure
**Description:** As a developer, I need one command to start all infrastructure so I don't waste time configuring each piece manually.

**Acceptance Criteria:**
- [ ] `docker-compose up -d` starts all infrastructure: mysql, redis, mongodb, kafka (KRaft mode, 1 broker), rabbitmq (with delayed message plugin), nacos, seata-server, zipkin, elasticsearch, logstash, kibana
- [ ] Each container has health check configured
- [ ] Services start in correct dependency order (data stores -> brokers -> infra -> observability)
- [ ] Total memory footprint stays under 10GB with default settings
- [ ] Kafka runs in KRaft mode (no ZooKeeper)

### US-003: Create MySQL init scripts for all databases
**Description:** As a developer, I need databases and tables created automatically when MySQL starts so I don't manually set up schemas.

**Acceptance Criteria:**
- [ ] Init scripts in `deploy/mysql/init/` create databases: `user_db`, `auction_db`, `ticket_db`, `order_db`, `seata_db`
- [ ] All tables from design doc schema section are created with correct columns, types, and indexes
- [ ] Seata tables (`branch_table`, `global_table`, `lock_table`) created in `seata_db`
- [ ] Scripts are idempotent (safe to re-run)

### US-004: Set up Nacos as service registry and config center
**Description:** As a developer, I need Nacos running so services can register themselves and pull shared configuration.

**Acceptance Criteria:**
- [ ] Nacos starts in standalone mode via Docker Compose
- [ ] Shared configs created in Nacos: `shared-db.yml`, `shared-redis.yml`, `shared-kafka.yml`
- [ ] Nacos console accessible at port 8848
- [ ] `shared-db.yml` content:
  ```yaml
  spring:
    datasource:
      driver-class-name: com.mysql.cj.jdbc.Driver
      url: jdbc:mysql://mysql:3306/${DB_NAME}?useSSL=false&serverTimezone=UTC
      username: root
      password: root
  ```
- [ ] `shared-redis.yml` content:
  ```yaml
  spring:
    data:
      redis:
        host: redis
        port: 6379
  ```
- [ ] `shared-kafka.yml` content:
  ```yaml
  spring:
    kafka:
      bootstrap-servers: kafka:9092
      producer:
        acks: all
        enable-idempotence: true
      consumer:
        auto-offset-reset: earliest
        enable-auto-commit: false
  ```

### US-005: Create gateway-service skeleton
**Description:** As a developer, I need a running API gateway that can route to downstream services so the project has a unified entry point.

**Acceptance Criteria:**
- [ ] gateway-service starts and registers with Nacos
- [ ] Routes configured for all service paths: `/api/auctions/**`, `/api/tickets/**`, `/api/orders/**`, `/api/users/**`, `/api/notifications/**`
- [ ] Global CORS configured for frontend development
- [ ] Request logging filter adds traceId to all incoming requests
- [ ] Returns 503 when downstream service is unavailable (graceful fallback)

### US-006: Create stub services for all microservices
**Description:** As a developer, I need skeleton projects for all 7 microservices so each can be developed independently.

**Acceptance Criteria:**
- [ ] Each service has standard Spring Boot structure: controller, service, domain, repository, config, event packages
- [ ] Each service has its own `application.yml` with Nacos discovery enabled
- [ ] Each service has its own `bootstrap.yml` pointing to Nacos config center
- [ ] Each service has Dockerfile for containerized build
- [ ] All services register with Nacos on startup

### US-007: Configure Seata Server
**Description:** As a developer, I need Seata Server running so distributed transactions can work in later phases.

**Acceptance Criteria:**
- [ ] Seata Server starts via Docker Compose and connects to MySQL `seata_db`
- [ ] Seata config file stored in `deploy/seata/`
- [ ] Seata console accessible at port 7091

## Functional Requirements

- FR-1: Maven parent POM manages Spring Boot 3.2.5, Spring Cloud 2023.0.1, Spring Cloud Alibaba 2022.0.0.0 versions
- FR-2: Docker Compose file defines all 17 containers with health checks
- FR-3: MySQL init scripts create 5 databases with all required tables and indexes
- FR-4: Nacos shared configuration covers MySQL, Redis, and Kafka connection settings
- FR-5: Gateway-service routes map `/api/<service>/**` to corresponding Nacos-registered service
- FR-6: Each microservice includes `spring-cloud-starter-alibaba-nacos-discovery` and `spring-cloud-starter-alibaba-nacos-config`
- FR-7: Kafka broker configured with KRaft mode (no ZooKeeper), Kafka image version 3.6.1
- FR-8: RabbitMQ has `rabbitmq_delayed_message_exchange` plugin enabled

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

## Port Allocation

| Service | Port |
|---------|------|
| gateway-service | 8080 |
| auction-service | 8081 |
| ticket-service | 8082 |
| order-service | 8083 |
| notification-service | 8084 |
| event-store-service | 8085 |
| user-service | 8086 |
| Nacos console | 8848 |
| Seata console | 7091 |
| Zipkin UI | 9411 |
| Sentinel Dashboard | 8090 |
| Kibana | 5601 |
| Elasticsearch | 9200 |
| Logstash TCP | 5000 |

## Non-Goals

- No business logic implementation in this phase
- No authentication/authorization yet (deferred to P1)
- No rate limiting rules yet (deferred to P7)
- No frontend work yet (deferred to P6)
- No CI/CD pipeline

## Technical Considerations

- Use KRaft mode for Kafka to avoid ZooKeeper overhead (saves ~2GB RAM)
- MySQL single instance with multiple databases (not separate containers per service)
- RabbitMQ delayed message plugin requires manual enable in Dockerfile or entrypoint
- Seata Server needs its own MySQL tables, init script must run before Seata starts

## Success Metrics

- `docker-compose up -d` brings all containers to healthy state within 3 minutes
- `mvn clean package` succeeds from project root
- Gateway forwards request to a stub service and receives response
- Nacos console shows all registered services

## Resolved Decisions

- Build tool: Maven (Spring Cloud Alibaba official examples and docs use Maven)
- Kafka version: 3.6.1 (KRaft mode requires 3.3+, 3.6.1 is latest stable)
