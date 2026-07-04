package com.auction.gateway.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("InternalApiBlockFilter - 服务间内部端点不得经网关暴露给终端用户")
class InternalApiBlockFilterTest {

    private InternalApiBlockFilter filter;
    private AtomicBoolean chainCalled;
    private GatewayFilterChain chain;

    @BeforeEach
    void setUp() {
        filter = new InternalApiBlockFilter();
        chainCalled = new AtomicBoolean(false);
        chain = exchange -> {
            chainCalled.set(true);
            return Mono.empty();
        };
    }

    private MockServerWebExchange exchangeFor(String path) {
        return MockServerWebExchange.from(MockServerHttpRequest.post(path).build());
    }

    @Test
    @DisplayName("ticket 内部端点 - 403 且不再路由")
    void ticketInternalPath_blocked() {
        MockServerWebExchange exchange = exchangeFor("/api/tickets/internal/settle-release");

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(chainCalled).isFalse();
    }

    @Test
    @DisplayName("order 内部端点 - 403 且不再路由")
    void orderInternalPath_blocked() {
        MockServerWebExchange exchange = exchangeFor("/api/orders/internal/auction");

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(chainCalled).isFalse();
    }

    @Test
    @DisplayName("以 /internal 结尾的路径同样封禁")
    void trailingInternalSegment_blocked() {
        MockServerWebExchange exchange = exchangeFor("/api/tickets/internal");

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(chainCalled).isFalse();
    }

    @Test
    @DisplayName("普通业务路径 - 正常放行")
    void normalPath_passesThrough() {
        MockServerWebExchange exchange = exchangeFor("/api/tickets/reserve");

        filter.filter(exchange, chain).block();

        assertThat(chainCalled).isTrue();
        assertThat(exchange.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("路径段里含 internal 字样但不是 internal 段 - 不误伤")
    void similarButDifferentSegment_passesThrough() {
        MockServerWebExchange exchange = exchangeFor("/api/tickets/international-events");

        filter.filter(exchange, chain).block();

        assertThat(chainCalled).isTrue();
    }
}
