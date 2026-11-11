package com.auction.gateway.filter;

import org.junit.jupiter.api.BeforeEach;
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

class CanonicalPathFilterTest {

    private CanonicalPathFilter filter;
    private AtomicBoolean chainCalled;
    private WebFilterChain chain;

    @BeforeEach
    void setUp() {
        filter = new CanonicalPathFilter();
        chainCalled = new AtomicBoolean(false);
        chain = exchange -> {
            chainCalled.set(true);
            return Mono.empty();
        };
    }

    /** URI.create keeps the path byte for byte; the String overloads of the mock builder do not. */
    private MockServerWebExchange exchangeFor(String rawPathAndQuery) {
        return MockServerWebExchange.from(MockServerHttpRequest
                .method(HttpMethod.GET, URI.create("http://localhost" + rawPathAndQuery)).build());
    }

    /** Every path shape the allow-list in application.yml serves, plus the gateway's own. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "/",
            "/api/users/register",
            "/api/users/me",
            "/api/auctions",
            "/api/auctions/hot",
            "/api/auctions/42",
            "/api/auctions/42/bids",
            "/api/admin/auctions",
            "/api/tickets/events/7",
            "/api/tickets/reserve",
            "/api/orders/9/pay",
            "/api/notifications/65f1c0ffee65f1c0ffee65f1/read",
            "/ws/notifications/info",
            "/ws/notifications/123/abcd0123/websocket",
            "/ws/notifications/123/abcd0123/xhr_streaming",
            "/actuator/health",
            "/a.b/c_d/e~f/g-h",
            "/api/x/...",
            // The query string is not part of the path and is passed on as it is.
            "/api/auctions?page=0&size=20",
            "/ws/notifications/info?t=1731400000000",
            "/api/auctions?q=a%20b;c/../d//e"
    })
    void canonicalPath_passesThrough(String path) {
        MockServerWebExchange exchange = exchangeFor(path);

        filter.filter(exchange, chain).block();

        assertThat(chainCalled).as("chain called for %s", path).isTrue();
        assertThat(exchange.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.BAD_REQUEST);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            // path parameters
            "/api/tickets/internal;x/settle-reserve",
            "/api/tickets/internal;/settle-reserve",
            "/api/tickets;x/internal/settle-reserve",
            "/api/tickets/reserve;x=1",
            "/api/tickets/1;x/confirm",
            "/api/tickets/internal;%2F/settle-reserve",
            // percent-encoding of any kind
            "/api/tickets/%69nternal/settle-reserve",
            "/api/tickets/internal%3Bx/settle-reserve",
            "/api/tickets/internal%2Fsettle-reserve",
            "/api/tickets/internal%00/settle-reserve",
            "/api/tickets/%2569nternal/settle-reserve",
            "/api/tickets/%72eserve",
            "/api/tickets/reserve%20",
            // empty segments, including a trailing slash
            "/api/tickets//reserve",
            "//api/tickets/reserve",
            "/api/auctions/42//bids",
            "/api/tickets/reserve/",
            "//",
            // dot segments
            "/api/tickets/./reserve",
            "/api/tickets/x/../reserve",
            "/ws/../api/notifications",
            "/api/tickets/..",
            "/.",
            "/..",
            // characters outside the unreserved set
            "/api/tickets/+1/confirm",
            "/api/auctions/42!",
            "/api/auctions/42,43",
            "/api/auctions/a=b",
            "/api/auctions/a@b",
            "/api/auctions/a:b",
            "/api/auctions/$1",
            "/api/auctions/(1)",
            "/api/auctions/1*",
            "/api/auctions/'1'",
            "/api/auctions/a&b"
    })
    void nonCanonicalPath_isRefusedWith400(String path) {
        MockServerWebExchange exchange = exchangeFor(path);

        filter.filter(exchange, chain).block();

        assertThat(chainCalled).as("chain called for %s", path).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .isEqualTo("{\"code\":400,\"message\":\"Request path is not in canonical form\"}");
    }

    @Test
    void emptyOrRelativePath_isNotCanonical() {
        // What an asterisk-form request-target ("OPTIONS *") or a missing path looks like.
        assertThat(CanonicalPathFilter.isCanonical("")).isFalse();
        assertThat(CanonicalPathFilter.isCanonical(null)).isFalse();
        assertThat(CanonicalPathFilter.isCanonical("api/tickets")).isFalse();
        assertThat(CanonicalPathFilter.isCanonical("*")).isFalse();
    }

    @Test
    void nonAsciiCharacters_areNotCanonical() {
        assertThat(CanonicalPathFilter.isCanonical("/api/auctions/١")).isFalse();
        assertThat(CanonicalPathFilter.isCanonical("/api/auctions/１")).isFalse();
        assertThat(CanonicalPathFilter.isCanonical("/api/auctions/1\u0000")).isFalse();
        assertThat(CanonicalPathFilter.isCanonical("/api/auctions/1 ")).isFalse();
        assertThat(CanonicalPathFilter.isCanonical("/api/auctions/a\\b")).isFalse();
    }

    @Test
    void runsBeforeEveryOtherFilter() {
        assertThat(filter.getOrder()).isEqualTo(Integer.MIN_VALUE);
        assertThat(new InternalApiBlockFilter().getOrder()).isGreaterThan(filter.getOrder());
    }
}
