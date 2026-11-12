package com.auction.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Refuses any request whose path is not written in canonical form, before routing.
 *
 * <p>The gateway decides what a path means and a service decides again, each with its own
 * parser. They agree on plain paths and disagree elsewhere, and every disagreement is a way
 * past a check made here:
 * <ul>
 *   <li>{@code /api/tickets/internal;x/settle-reserve}: the old internal-path filter compared
 *       the segment {@code internal;x} with {@code internal} and let it through, while
 *       Spring MVC drops {@code ;x} and runs the internal handler.</li>
 *   <li>{@code /api/tickets//reserve}: route predicates see the empty segment, but the
 *       gateway removes it when it builds the outgoing URL, so the request missed the
 *       rate-limited route and still arrived as {@code /api/tickets/reserve}.</li>
 *   <li>{@code %69nternal}, {@code /x/../}, a trailing slash: each is read one way by some
 *       component and another way by the next.</li>
 * </ul>
 * Normalising here would only add a third parser that has to agree with the other two. A
 * canonical path is one all of them read the same way, so anything else is refused with
 * 400: the path must be {@code /} followed by segments of unreserved characters
 * ({@code A-Z a-z 0-9 - . _ ~}), with no empty segment and no {@code .} or {@code ..}
 * segment. That leaves no percent-encoding, no {@code ;} parameters and no trailing slash.
 * Every path this API serves fits; the query string is not looked at.
 *
 * <p>This is a {@link WebFilter}, not a gateway {@code GlobalFilter}: global filters run
 * only after a route has matched, and this check has to hold for every request, including
 * those to the gateway's own endpoints.
 */
@Component
public class CanonicalPathFilter implements WebFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(CanonicalPathFilter.class);

    private static final int MAX_LOGGED_PATH_LENGTH = 200;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String rawPath = exchange.getRequest().getURI().getRawPath();
        if (isCanonical(rawPath)) {
            return chain.filter(exchange);
        }
        log.warn("Refused non-canonical path: {} {}", exchange.getRequest().getMethod(), abbreviate(rawPath));
        return GatewayErrors.write(exchange, HttpStatus.BAD_REQUEST, "Request path is not in canonical form");
    }

    static boolean isCanonical(String rawPath) {
        if (rawPath == null || rawPath.isEmpty() || rawPath.charAt(0) != '/') {
            return false;
        }
        if (rawPath.length() == 1) {
            return true;
        }
        int segmentStart = 1;
        for (int i = 1; i <= rawPath.length(); i++) {
            if (i == rawPath.length() || rawPath.charAt(i) == '/') {
                if (!isPlainSegment(rawPath, segmentStart, i)) {
                    return false;
                }
                segmentStart = i + 1;
            }
        }
        return true;
    }

    private static boolean isPlainSegment(String path, int start, int end) {
        int length = end - start;
        if (length == 0) {
            return false;
        }
        if (path.charAt(start) == '.' && (length == 1 || (length == 2 && path.charAt(start + 1) == '.'))) {
            return false;
        }
        for (int i = start; i < end; i++) {
            if (!isUnreserved(path.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** RFC 3986 "unreserved": the characters that never need encoding and carry no syntax. */
    private static boolean isUnreserved(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                || c == '-' || c == '.' || c == '_' || c == '~';
    }

    private static String abbreviate(String rawPath) {
        if (rawPath == null || rawPath.length() <= MAX_LOGGED_PATH_LENGTH) {
            return rawPath;
        }
        return rawPath.substring(0, MAX_LOGGED_PATH_LENGTH) + "...";
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
