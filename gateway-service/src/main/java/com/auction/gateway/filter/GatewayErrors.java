package com.auction.gateway.filter;

import com.auction.common.security.GatewayHeaders;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Writes the error body for the requests the path filters refuse themselves, in the same
 * envelope every other API error uses (code, message, timestamp, traceId).
 *
 * <p>The JSON is assembled by hand so the filters need no ObjectMapper. That is safe only
 * because nothing from the request goes in unchecked: the message is a fixed string from
 * this codebase, and the trace id is echoed only when it is plain enough to need no
 * escaping.
 */
final class GatewayErrors {

    private static final Pattern PLAIN_TRACE_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private GatewayErrors() {
    }

    static Mono<Void> write(ServerWebExchange exchange, HttpStatus status, String message) {
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"code\":" + status.value()
                + ",\"message\":\"" + message + "\""
                + ",\"timestamp\":\"" + Instant.now() + "\""
                + ",\"traceId\":\"" + traceId(exchange) + "\"}";
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    private static String traceId(ServerWebExchange exchange) {
        String supplied = exchange.getRequest().getHeaders().getFirst(GatewayHeaders.TRACE_ID);
        return supplied != null && PLAIN_TRACE_ID.matcher(supplied).matches() ? supplied : "unknown";
    }
}
