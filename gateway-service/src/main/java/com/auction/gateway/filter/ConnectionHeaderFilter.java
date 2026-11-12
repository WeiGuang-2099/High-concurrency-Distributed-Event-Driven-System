package com.auction.gateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Stops a client deciding which headers the gateway forwards.
 *
 * <p>A proxy must not forward the headers a request lists in its {@code Connection}
 * header, and Spring Cloud Gateway follows that rule when it builds the outgoing request
 * (RemoveHopByHopHeadersFilter). The list comes from the client, though, and the removal
 * happens after every filter here has run. A request with a valid token and
 * {@code Connection: X-User-Id, X-User-Roles} therefore reached the service with the
 * identity that {@link JwtAuthenticationFilter} had just derived from the token deleted
 * again: authenticated at the gateway, anonymous at the service. Naming
 * {@code Authorization} or any other header worked the same way.
 *
 * <p>The header is cut down to the options that describe the connection itself
 * ({@code close}, {@code keep-alive}, {@code upgrade}); every other token names a header,
 * and no client needs the gateway to drop one on its behalf. The request is not refused,
 * because ordinary clients do send extra tokens: the JDK HTTP client, for one, sends
 * {@code Connection: Upgrade, HTTP2-Settings} on every plain-HTTP request.
 */
@Component
public class ConnectionHeaderFilter implements GlobalFilter, Ordered {

    private static final Set<String> CONNECTION_OPTIONS = Set.of("close", "keep-alive", "upgrade");

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        List<String> declared = exchange.getRequest().getHeaders().getConnection();
        List<String> kept = declared.stream().filter(ConnectionHeaderFilter::isConnectionOption).toList();
        if (kept.size() == declared.size()) {
            return chain.filter(exchange);
        }
        ServerWebExchange sanitized = exchange.mutate()
                .request(builder -> builder.headers(headers -> {
                    if (kept.isEmpty()) {
                        headers.remove(HttpHeaders.CONNECTION);
                    } else {
                        headers.setConnection(kept);
                    }
                }))
                .build();
        return chain.filter(sanitized);
    }

    private static boolean isConnectionOption(String token) {
        return CONNECTION_OPTIONS.contains(token.trim().toLowerCase(Locale.ROOT));
    }

    /** Any position before the routing filter works; first keeps it out of the way. */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
