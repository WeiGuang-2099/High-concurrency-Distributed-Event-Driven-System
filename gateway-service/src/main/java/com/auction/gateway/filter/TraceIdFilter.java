package com.auction.gateway.filter;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Echoes the active Micrometer/Brave trace id to the client as the {@code X-Trace-Id}
 * response header, so a caller can paste it into Zipkin (the trace) or Kibana (the logs
 * for that request across services).
 *
 * <p>The trace itself is created at the edge and propagated downstream via B3 headers
 * automatically by Micrometer Tracing; this filter no longer mints its own id. Brave also
 * puts the same id into the SLF4J MDC, which is what links a Zipkin trace to its logs.
 */
@Component
public class TraceIdFilter implements GlobalFilter, Ordered {

    private static final String TRACE_ID_HEADER = "X-Trace-Id";

    private final Tracer tracer;

    public TraceIdFilter(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // Set just before the response commits, by which point the trace span is active.
        exchange.getResponse().beforeCommit(() -> {
            Span span = tracer.currentSpan();
            if (span != null) {
                exchange.getResponse().getHeaders()
                        .set(TRACE_ID_HEADER, span.context().traceId());
            }
            return Mono.empty();
        });
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
