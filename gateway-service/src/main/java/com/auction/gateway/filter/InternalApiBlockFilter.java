package com.auction.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * Rejects any request targeting a service-to-service internal endpoint
 * (/api/&#42;/internal/&#42;&#42;) before authentication and routing. These endpoints
 * (settle-reserve, settle-release, internal order creation) implement saga steps and
 * trust their callers; the downstream routes forward /api/&lt;service&gt;/** wholesale, so
 * without this filter ANY logged-in user could invoke them through the gateway.
 *
 * <p>Runs at HIGHEST_PRECEDENCE, ahead of {@link JwtAuthenticationFilter}: an internal
 * path is forbidden regardless of who asks, so there is no point verifying the token.
 */
@Component
public class InternalApiBlockFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(InternalApiBlockFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();
        if (containsInternalSegment(path)) {
            log.warn("Blocked external access to internal endpoint: {}", path);
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
            byte[] body = "{\"code\":403,\"message\":\"Forbidden\"}".getBytes(StandardCharsets.UTF_8);
            DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body);
            return exchange.getResponse().writeWith(Mono.just(buffer));
        }
        return chain.filter(exchange);
    }

    // Match "internal" as a whole path segment, not as a substring of another segment.
    private boolean containsInternalSegment(String path) {
        for (String segment : path.split("/")) {
            if ("internal".equals(segment)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
