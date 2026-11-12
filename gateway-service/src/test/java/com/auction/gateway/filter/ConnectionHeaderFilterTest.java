package com.auction.gateway.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.headers.RemoveHopByHopHeadersFilter;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionHeaderFilterTest {

    private ConnectionHeaderFilter filter;
    private AtomicReference<ServerWebExchange> forwarded;
    private GatewayFilterChain chain;

    @BeforeEach
    void setUp() {
        filter = new ConnectionHeaderFilter();
        forwarded = new AtomicReference<>();
        chain = exchange -> {
            forwarded.set(exchange);
            return Mono.empty();
        };
    }

    private HttpHeaders headersAfterFilter(MockServerHttpRequest.BaseBuilder<?> request) {
        filter.filter(MockServerWebExchange.from(request.build()), chain).block();
        return forwarded.get().getRequest().getHeaders();
    }

    @Test
    void headerNamesAreDroppedFromConnection_soTheGatewayKeepsForwardingThoseHeaders() {
        MockServerHttpRequest.BaseBuilder<?> request = MockServerHttpRequest.get("/api/orders")
                .header("Connection", "X-User-Id, x-user-roles, Authorization, close")
                .header("X-User-Id", "4343")
                .header("X-User-Roles", "USER")
                .header("Authorization", "Bearer token");

        HttpHeaders headers = headersAfterFilter(request);

        assertThat(headers.getConnection()).containsExactly("close");
        // The gateway's own hop-by-hop filter, applied to what this filter lets through,
        // is what would otherwise delete them.
        HttpHeaders outgoing = new RemoveHopByHopHeadersFilter().filter(headers, forwarded.get());
        assertThat(outgoing.getFirst("X-User-Id")).isEqualTo("4343");
        assertThat(outgoing.getFirst("X-User-Roles")).isEqualTo("USER");
        assertThat(outgoing.getFirst("Authorization")).isEqualTo("Bearer token");
    }

    @Test
    void withoutThisFilter_theGatewayWouldRemoveWhateverTheClientNames() {
        // Pins the framework behaviour this filter exists for. If a future Spring Cloud
        // Gateway stops honouring client-named tokens, this test says so.
        HttpHeaders headers = new HttpHeaders();
        headers.add("Connection", "X-User-Id, close");
        headers.add("X-User-Id", "4343");
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/orders").build());

        HttpHeaders outgoing = new RemoveHopByHopHeadersFilter().filter(headers, exchange);

        assertThat(outgoing.containsKey("X-User-Id")).isFalse();
    }

    @Test
    void headerNamesSpreadOverSeveralConnectionLines_areAllDropped() {
        MockServerHttpRequest.BaseBuilder<?> request = MockServerHttpRequest.get("/api/orders")
                .header("Connection", "keep-alive")
                .header("Connection", "X-User-Id")
                .header("Connection", " X-Username ,X-User-Roles");

        assertThat(headersAfterFilter(request).getConnection()).containsExactly("keep-alive");
    }

    @Test
    void connectionWithOnlyHeaderNames_isRemovedAltogether() {
        MockServerHttpRequest.BaseBuilder<?> request = MockServerHttpRequest.get("/api/orders")
                .header("Connection", "X-User-Id, HTTP2-Settings");

        assertThat(headersAfterFilter(request).containsKey(HttpHeaders.CONNECTION)).isFalse();
    }

    @Test
    void webSocketUpgradeHandshake_isLeftAsItIs() {
        MockServerHttpRequest.BaseBuilder<?> request = MockServerHttpRequest
                .get("/ws/notifications/123/abcd0123/websocket")
                .header("Connection", "keep-alive, Upgrade")
                .header("Upgrade", "websocket");

        HttpHeaders headers = headersAfterFilter(request);

        assertThat(headers.getConnection()).containsExactly("keep-alive", "Upgrade");
        assertThat(headers.getUpgrade()).isEqualTo("websocket");
    }

    @Test
    void requestWithoutConnectionHeader_isPassedOnUntouched() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/orders").build());

        filter.filter(exchange, chain).block();

        assertThat(forwarded.get()).isSameAs(exchange);
    }
}
