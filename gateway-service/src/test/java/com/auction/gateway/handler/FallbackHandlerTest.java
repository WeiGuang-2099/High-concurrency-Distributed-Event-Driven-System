package com.auction.gateway.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.support.NotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.assertThat;

class FallbackHandlerTest {

    private FallbackHandler handler;

    @BeforeEach
    void setUp() {
        handler = new FallbackHandler();
        ReflectionTestUtils.setField(handler, "objectMapper", new ObjectMapper().registerModule(new JavaTimeModule()));
    }

    private MockServerWebExchange exchange() {
        return MockServerWebExchange.from(MockServerHttpRequest.get("/api/tickets/1/confirm").build());
    }

    @Test
    void noMatchingRoute_is404_notAnOutage() {
        MockServerWebExchange exchange = exchange();

        handler.handle(exchange, new ResponseStatusException(HttpStatus.NOT_FOUND, "No static resource x")).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        String body = exchange.getResponse().getBodyAsString().block();
        assertThat(body).contains("\"code\":404").contains("\"message\":\"Not found\"");
        // The exception's reason can carry the requested path; it is not echoed back.
        assertThat(body).doesNotContain("No static resource");
    }

    @Test
    void otherClientErrors_keepTheirStatus() {
        MockServerWebExchange exchange = exchange();

        handler.handle(exchange, new ResponseStatusException(HttpStatus.METHOD_NOT_ALLOWED)).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"code\":405").contains("\"message\":\"Method Not Allowed\"");
    }

    @Test
    void noInstanceOfTheService_isStill503() {
        MockServerWebExchange exchange = exchange();

        // What the load-balancer filter raises when a listed route has no live instance.
        handler.handle(exchange, NotFoundException.create(false, "Unable to find instance for ticket-service"))
                .block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"code\":503").contains("Service temporarily unavailable");
    }

    @Test
    void unexpectedFailure_isStill503() {
        MockServerWebExchange exchange = exchange();

        handler.handle(exchange, new ConnectException("Connection refused")).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(exchange.getResponse().getBodyAsString().block()).contains("\"code\":503");
    }

    @Test
    void serverSideStatusException_isReportedAs503() {
        MockServerWebExchange exchange = exchange();

        handler.handle(exchange, new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "boom")).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }
}
