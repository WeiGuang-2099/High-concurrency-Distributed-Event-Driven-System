package com.auction.gateway.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.auction.common.dto.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Turns an unhandled gateway error into the JSON error envelope.
 *
 * <p>A client error keeps its own status. In particular a request that matches no route is
 * 404: the route table is an allow-list, so "not listed" is the normal answer to most of
 * what an unknown caller sends, and it must not look like an outage. This handler used to
 * answer 503 "Service temporarily unavailable" to everything and log a full stack trace for
 * each, so probing unlisted paths both misreported the system as down and flooded the log.
 *
 * <p>Everything else (no instance of the service, connection refused, Redis down, a bug) is
 * still 503 and still logged with its cause.
 */
@Order(-1)
@Component
public class FallbackHandler implements ErrorWebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(FallbackHandler.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(ex);
        }

        HttpStatusCode status;
        String message;
        if (ex instanceof ResponseStatusException statusException
                && statusException.getStatusCode().is4xxClientError()) {
            status = statusException.getStatusCode();
            message = clientErrorMessage(status);
            // No stack trace and not at WARN: anyone can produce these at will.
            log.debug("Gateway request refused: {} {} -> {}",
                    exchange.getRequest().getMethod(), exchange.getRequest().getPath(), status);
        } else {
            status = HttpStatus.SERVICE_UNAVAILABLE;
            message = "Service temporarily unavailable";
            // Log the real cause; otherwise every gateway routing failure is invisible.
            log.warn("Gateway request failed: {} {} -> {}",
                    exchange.getRequest().getMethod(), exchange.getRequest().getPath(),
                    ex.toString(), ex);
        }

        String traceId = exchange.getRequest().getHeaders().getFirst("X-Trace-Id");
        if (traceId == null) {
            traceId = "unknown";
        }

        ErrorResponse errorResponse = ErrorResponse.of(status.value(), message, traceId);

        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(errorResponse);
        } catch (JsonProcessingException e) {
            bytes = ("{\"code\":" + status.value() + ",\"message\":\"" + message + "\"}").getBytes();
        }

        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    /** A fixed phrase per status; the exception's own reason can echo request data. */
    private static String clientErrorMessage(HttpStatusCode status) {
        if (status.value() == HttpStatus.NOT_FOUND.value()) {
            return "Not found";
        }
        HttpStatus known = HttpStatus.resolve(status.value());
        return known != null ? known.getReasonPhrase() : "Request refused";
    }
}
