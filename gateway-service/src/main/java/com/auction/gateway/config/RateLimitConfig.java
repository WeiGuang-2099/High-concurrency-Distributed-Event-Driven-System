package com.auction.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

/**
 * Rate limiting for the hot write paths (ticket reserve, auction bid). The routes in
 * application.yml attach Spring Cloud Gateway's RequestRateLimiter with the Redis
 * token-bucket implementation, so the budget is enforced per user and shared across
 * gateway instances; over-limit requests are rejected with 429 before reaching the
 * downstream service.
 */
@Configuration
public class RateLimitConfig {

    /**
     * Buckets are keyed by the X-User-Id header. JwtAuthenticationFilter runs before
     * route filters and sets that header from the verified JWT (replacing any
     * client-supplied value), so the key cannot be spoofed on authenticated routes.
     * The IP fallback only applies to unauthenticated paths, none of which currently
     * carry a rate limiter.
     */
    @Bean
    public KeyResolver userKeyResolver() {
        return exchange -> {
            String userId = exchange.getRequest().getHeaders().getFirst("X-User-Id");
            if (userId != null && !userId.isBlank()) {
                return Mono.just("user:" + userId);
            }
            InetSocketAddress remote = exchange.getRequest().getRemoteAddress();
            return Mono.just(remote != null ? "ip:" + remote.getAddress().getHostAddress() : "unknown");
        };
    }
}
