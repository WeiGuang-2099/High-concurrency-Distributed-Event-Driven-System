package com.auction.gateway.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The filter on its own, with no CanonicalPathFilter in front of it: it has to recognise an
 * internal segment however the segment is spelled, because it is the second lock and must
 * not depend on the first.
 */
@DisplayName("InternalApiBlockFilter - 服务间内部端点不得经网关暴露给终端用户")
class InternalApiBlockFilterTest {

    private InternalApiBlockFilter filter;
    private AtomicBoolean chainCalled;
    private WebFilterChain chain;

    @BeforeEach
    void setUp() {
        filter = new InternalApiBlockFilter();
        chainCalled = new AtomicBoolean(false);
        chain = exchange -> {
            chainCalled.set(true);
            return Mono.empty();
        };
    }

    /**
     * URI.create, not MockServerHttpRequest.post(String): the String form encodes "%" again
     * and collapses "//", so "%69nternal" would reach the filter as "%2569nternal".
     */
    private MockServerWebExchange exchangeFor(String rawPath) {
        return MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST, URI.create("http://localhost" + rawPath)).build());
    }

    @ParameterizedTest(name = "{0}")
    @DisplayName("内部端点 - 403 且不再路由")
    @ValueSource(strings = {
            "/api/tickets/internal/settle-reserve",
            "/api/tickets/internal/settle-release",
            "/api/orders/internal/auction",
            "/api/tickets/internal",
            "/internal",
            "/ticket-service/api/tickets/internal/settle-reserve"
    })
    void internalPath_blocked(String path) {
        MockServerWebExchange exchange = exchangeFor(path);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(chainCalled).isFalse();
    }

    /**
     * Every spelling below is one that Spring MVC downstream resolves to the "internal"
     * segment (observed against Tomcat 10.1 + Spring MVC 6.1 with default path matching).
     * The first three got past the previous version of this filter.
     */
    @ParameterizedTest(name = "{0}")
    @DisplayName("下游会当作 internal 的其他写法 - 同样 403")
    @ValueSource(strings = {
            "/api/tickets/internal;x/settle-reserve",
            "/api/tickets/internal;/settle-reserve",
            "/api/tickets/internal;x=1;y=2/settle-reserve",
            "/api/orders/internal;x/auction",
            "/api/tickets/internal;%2F/settle-reserve",
            "/api/tickets/%69nternal/settle-reserve",
            "/api/tickets/%69nternal;x/settle-reserve",
            "/api/tickets;x/internal/settle-reserve",
            "/api/tickets/internal/settle-reserve;x",
            "/api/tickets//internal/settle-reserve"
    })
    void internalPath_inAnotherSpelling_blocked(String path) {
        MockServerWebExchange exchange = exchangeFor(path);

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(chainCalled).isFalse();
    }

    @Test
    @DisplayName("大小写不同的 internal - 同样封禁")
    void internalSegment_inAnotherCase_blocked() {
        MockServerWebExchange exchange = exchangeFor("/api/tickets/INTERNAL/settle-reserve");

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

    @ParameterizedTest(name = "{0}")
    @DisplayName("路径段里含 internal 字样但不是 internal 段 - 不误伤")
    @ValueSource(strings = {
            "/api/tickets/international-events",
            "/api/tickets/internal-notes",
            "/api/tickets/xinternal",
            "/api/auctions?filter=internal"
    })
    void similarButDifferentSegment_passesThrough(String path) {
        MockServerWebExchange exchange = exchangeFor(path);

        filter.filter(exchange, chain).block();

        assertThat(chainCalled).isTrue();
    }
}
