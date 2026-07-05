package com.auction.auction.it;

import com.auction.auction.client.OrderFeignClient;
import com.auction.auction.client.TicketFeignClient;
import com.auction.auction.service.SettlementService;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Settlement saga orchestration over real HTTP: both Feign clients resolve to a
 * WireMock server (via the simple discovery client), so the compensation flow is
 * exercised through the same Feign + LoadBalancer stack used in production.
 */
@SpringBootTest(classes = SettlementSagaIT.SagaTestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "management.tracing.enabled=false")
class SettlementSagaIT {

    private static final String RESERVE_URL = "/api/tickets/internal/settle-reserve";
    private static final String RELEASE_URL = "/api/tickets/internal/settle-release";
    private static final String ORDER_URL = "/api/orders/internal/auction";

    private static final WireMockServer WIRE_MOCK =
            new WireMockServer(WireMockConfiguration.options().dynamicPort());

    static {
        WIRE_MOCK.start();
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            RedisAutoConfiguration.class,
            KafkaAutoConfiguration.class})
    @EnableFeignClients(clients = {TicketFeignClient.class, OrderFeignClient.class})
    @Import(SettlementService.class)
    static class SagaTestApp {
    }

    @DynamicPropertySource
    static void wireMockAsBothServices(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.discovery.client.simple.instances.ticket-service[0].uri",
                WIRE_MOCK::baseUrl);
        registry.add("spring.cloud.discovery.client.simple.instances.order-service[0].uri",
                WIRE_MOCK::baseUrl);
    }

    @AfterAll
    static void stopWireMock() {
        WIRE_MOCK.stop();
    }

    @Autowired
    private SettlementService settlementService;

    @BeforeEach
    void resetStubs() {
        WIRE_MOCK.resetAll();
        WIRE_MOCK.stubFor(post(urlEqualTo(RESERVE_URL)).willReturn(okJson(
                apiResponse(200, "{\"reservationId\":42}"))));
        WIRE_MOCK.stubFor(post(urlEqualTo(RELEASE_URL)).willReturn(okJson(
                apiResponse(200, "null"))));
    }

    @Test
    void settle_happyPath_reservesThenOrders_withoutCompensation() {
        WIRE_MOCK.stubFor(post(urlEqualTo(ORDER_URL)).willReturn(okJson(
                apiResponse(200, "{\"orderId\":7}"))));

        settlementService.settle(1L, 2L, new BigDecimal("99.90"), 3L);

        WIRE_MOCK.verify(1, postRequestedFor(urlEqualTo(RESERVE_URL))
                .withRequestBody(matchingJsonPath("$.ticketTypeId", equalTo("3")))
                .withRequestBody(matchingJsonPath("$.quantity", equalTo("1"))));
        WIRE_MOCK.verify(1, postRequestedFor(urlEqualTo(ORDER_URL))
                .withRequestBody(matchingJsonPath("$.winnerId", equalTo("2"))));
        WIRE_MOCK.verify(0, postRequestedFor(urlEqualTo(RELEASE_URL)));
    }

    @Test
    void settle_orderBusinessFailure_releasesReservation() {
        WIRE_MOCK.stubFor(post(urlEqualTo(ORDER_URL)).willReturn(okJson(
                apiResponse(500, "null"))));

        assertThatThrownBy(() -> settlementService.settle(1L, 2L, new BigDecimal("99.90"), 3L))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Order creation failed");

        WIRE_MOCK.verify(1, postRequestedFor(urlEqualTo(RELEASE_URL))
                .withRequestBody(matchingJsonPath("$.reservationId", equalTo("42"))));
    }

    @Test
    void settle_orderHttpError_releasesReservation() {
        WIRE_MOCK.stubFor(post(urlEqualTo(ORDER_URL)).willReturn(serverError()));

        assertThatThrownBy(() -> settlementService.settle(1L, 2L, new BigDecimal("99.90"), 3L))
                .isInstanceOf(RuntimeException.class);

        WIRE_MOCK.verify(1, postRequestedFor(urlEqualTo(RELEASE_URL))
                .withRequestBody(matchingJsonPath("$.reservationId", equalTo("42"))));
    }

    private static String apiResponse(int code, String dataJson) {
        return "{\"code\":" + code + ",\"message\":\"test\",\"data\":" + dataJson + "}";
    }
}
