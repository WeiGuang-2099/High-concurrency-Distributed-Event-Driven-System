package com.auction.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.PathContainer;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Refuses any request that names a service-to-service endpoint ({@code /api/&#42;/internal/&#42;&#42;}).
 * Those endpoints (settle-reserve, settle-release, internal order creation) are saga steps
 * that trust their caller completely: they take the user id and the amount from the body.
 *
 * <p>The route table in application.yml is an allow-list and lists none of them, so this
 * filter is the second lock, not the first. It is there for the day someone adds a route
 * broad enough to cover an internal path.
 *
 * <p>Two things the earlier version got wrong, both found by sending it real requests:
 * <ul>
 *   <li>It compared raw path segments, so {@code internal;x} was not {@code internal}, while
 *       Spring MVC downstream drops the {@code ;x} and runs the handler. It now compares
 *       what the path matcher compares: the decoded segment with its parameters removed.
 *       {@link CanonicalPathFilter} already refuses such paths; this filter does not rely
 *       on that.</li>
 *   <li>It was a gateway {@code GlobalFilter}, and those run only for requests that matched
 *       a route. As a {@link WebFilter} it sees every request.</li>
 * </ul>
 */
@Component
public class InternalApiBlockFilter implements WebFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(InternalApiBlockFilter.class);

    private static final String INTERNAL_SEGMENT = "internal";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // The whole path, context path included: it is what the route predicate matches and
        // what gets forwarded. Behind a proxy that sets X-Forwarded-Prefix (with
        // server.forward-headers-strategy=framework) the prefix becomes the context path,
        // and judging only the part after it would let the prefix carry the segment.
        PathContainer path = exchange.getRequest().getPath();
        if (hasInternalSegment(path)) {
            log.warn("Blocked external access to internal endpoint: {} {}",
                    exchange.getRequest().getMethod(), exchange.getRequest().getPath().value());
            return GatewayErrors.write(exchange, HttpStatus.FORBIDDEN, "Forbidden");
        }
        return chain.filter(exchange);
    }

    /**
     * A whole segment equal to "internal", so /api/tickets/international-events passes.
     * Case is ignored: no service maps another spelling, and refusing them costs nothing.
     */
    private static boolean hasInternalSegment(PathContainer path) {
        for (PathContainer.Element element : path.elements()) {
            if (element instanceof PathContainer.PathSegment segment
                    && INTERNAL_SEGMENT.equalsIgnoreCase(segment.valueToMatch())) {
                return true;
            }
        }
        return false;
    }

    /** Right after {@link CanonicalPathFilter}, ahead of everything else. */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }
}
