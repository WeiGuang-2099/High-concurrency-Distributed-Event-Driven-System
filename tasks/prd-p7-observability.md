# PRD: P7 - Observability (ELK + Zipkin + Sentinel)

## Introduction

实现可观测性和流量治理：ELK 日志聚合、Micrometer Tracing + Zipkin 分布式链路追踪、Sentinel 限流降级。面试中面试官会问"线上出了问题你怎么排查"和"流量大了怎么办"，这个阶段让项目有完整的可观测性故事。

## Goals

- 所有微服务日志通过 ELK 统一收集和检索
- 全链路追踪：一个请求从 Gateway 到 DB 的完整调用链可查
- Sentinel 限流保护 Gateway 和核心服务
- Kibana 预设 Dashboard 展示系统健康状态

## User Stories

### US-001: Structured JSON logging via Logback
**Description:** As a developer, I need all services to output structured JSON logs so Logstash can parse them uniformly.

**Acceptance Criteria:**
- [ ] Each microservice configures `logstash-logback-encoder` in `logback-spring.xml`
- [ ] Log output includes: timestamp, level, service name, traceId, spanId, message, mdc fields
- [ ] traceId propagated from Gateway through all downstream calls
- [ ] Logs written to stdout (Docker captures and sends to Logstash)

### US-002: Logstash pipeline for log collection
**Description:** As the system, I need Logstash to collect, parse, and forward logs to Elasticsearch.

**Acceptance Criteria:**
- [ ] Logstash config in `deploy/logstash/pipeline/logstash.conf`
- [ ] Input: Docker log driver (or TCP input on port 5000)
- [ ] Filter: parse JSON, extract traceId/spanId/service fields, add timestamp
- [ ] Output: Elasticsearch index pattern `auction-logs-{YYYY.MM.dd}`
- [ ] Logstash starts after Elasticsearch is healthy

### US-003: Elasticsearch and Kibana setup
**Description:** As a developer, I need Elasticsearch to store logs and Kibana to visualize them.

**Acceptance Criteria:**
- [ ] Elasticsearch starts with 1.5GB heap, creates index template for auction-logs
- [ ] Kibana connects to Elasticsearch, accessible at port 5601
- [ ] Index pattern `auction-logs-*` auto-created

### US-004: Kibana preset dashboards
**Description:** As a developer, I want preset dashboards in Kibana so I can quickly see system health without building queries.

**Acceptance Criteria:**
- [ ] Dashboard 1 - "Error Overview": error rate by service, top 10 error messages, error timeline
- [ ] Dashboard 2 - "API Performance": P50/P95/P99 latency by endpoint, slowest requests
- [ ] Dashboard 3 - "Kafka Health": consumer lag by topic, message throughput
- [ ] Dashboard 4 - "Auction Activity": bids per second, active auctions, bid success rate
- [ ] Dashboard JSON exports stored in `deploy/kibana/dashboards/`

### US-005: Distributed tracing with Micrometer + Zipkin
**Description:** As a developer, I need to trace a request from Gateway through multiple services to find performance bottlenecks.

**Acceptance Criteria:**
- [ ] All services include `micrometer-tracing-bridge-brave` + `zipkin-reporter` dependencies
- [ ] Zipkin server runs in Docker Compose, accessible at port 9411
- [ ] traceId propagates through: HTTP headers (Gateway -> service), Kafka headers (producer -> consumer), RabbitMQ headers
- [ ] Zipkin UI shows full trace with spans for each service call
- [ ] Can search traces by traceId, service name, or endpoint

### US-006: Sentinel rate limiting at Gateway
**Description:** As the system, I need to limit request rates at the gateway so backend services are not overwhelmed during traffic spikes.

**Acceptance Criteria:**
- [ ] Sentinel dependency added to gateway-service
- [ ] Flow rules configured in Nacos (dynamic, no restart needed):
  - `/api/auctions/{id}/bids`: 500 QPS per IP
  - `/api/tickets/reserve`: 200 QPS per IP
  - Global: 2000 QPS total
- [ ] Rate-limited requests return 429 with "Too many requests" message
- [ ] Sentinel dashboard runs as standalone container in Docker Compose (image: bladex/sentinel-dashboard:1.8.6, port 8090), separate from gateway-service
- [ ] Gateway-service pushes metrics to Sentinel dashboard via configured address in Nacos

### US-007: Sentinel circuit breaker for service calls
**Description:** As the system, I need circuit breakers on inter-service calls so a failing downstream doesn't cascade.

**Acceptance Criteria:**
- [ ] OpenFeign clients configured with Sentinel fallback
- [ ] If downstream service fails > 50% in 10s window: circuit opens, calls go to fallback
- [ ] Fallback returns cached response or graceful error (not exception)
- [ ] Circuit auto-closes after 30s of being open (half-open state)

## Functional Requirements

- FR-1: All services output JSON logs with traceId/spanId via Logback
- FR-2: Logstash pipeline parses and forwards to Elasticsearch
- FR-3: Kibana has 4 preset dashboards for system monitoring
- FR-4: Micrometer Tracing + Zipkin provides full distributed trace visibility
- FR-5: traceId propagates across HTTP, Kafka, and RabbitMQ boundaries
- FR-6: Sentinel flow rules limit QPS at gateway for critical endpoints
- FR-7: Sentinel circuit breakers protect OpenFeign inter-service calls
- FR-8: All rate limiting rules stored in Nacos for dynamic updates

## Non-Goals

- No Prometheus + Grafana (ELK + Zipkin + Sentinel covers logging, tracing, and metrics)
- No alerting system ( PagerDuty, Slack webhook)
- No custom metrics exporters
- No log archiving strategy

## Technical Considerations

- Spring Boot 3.x uses Micrometer Tracing (not Sleuth) with Brave bridge
- Kafka trace propagation: Spring Cloud Stream auto-propagates tracing headers if configured
- RabbitMQ trace propagation: Spring AMQP supports `SpringAmqpTracing` auto-config
- Sentinel rules in Nacos use `dataId: sentinel-gateway-flow-rules`, `group: SENTINEL_GROUP`
- Logstash TCP input is simpler than Docker log driver for development

## Success Metrics

- Search logs by traceId in Kibana returns all related log entries across services
- Zipkin trace shows full call chain: Gateway -> service -> DB with latency per span
- Rate limiting triggers 429 responses when QPS exceeds configured threshold
- Circuit breaker opens within 10 seconds of downstream failure
