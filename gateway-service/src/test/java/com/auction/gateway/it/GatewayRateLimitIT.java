package com.auction.gateway.it;

import com.auction.gateway.it.support.RawHttp;
import com.auction.gateway.it.support.TestTokens;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * End-to-end test of the per-user Redis token bucket on the hot write routes:
 * boots the full gateway (JWT auth + rate limiter) against a real Redis container,
 * with WireMock standing in for ticket-service and auction-service via the simple
 * discovery client. Tokens are signed with the real user-service keystore so the
 * whole JwtAuthenticationFilter -> KeyResolver -> RedisRateLimiter chain runs.
 *
 * <p>Limits are tightened to replenish=1/s, burst=3 (via the rate-limit.* knobs the
 * routes expose) so the bucket demonstrably empties within a handful of requests.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "rate-limit.reserve.replenish=1",
                "rate-limit.reserve.burst=3",
                "rate-limit.bid.replenish=1",
                "rate-limit.bid.burst=3",
                // Hermetic stand-ins only: no config-server fetch, no Eureka. The route
                // table and the discovery-locator setting are the shipped ones. This test
                // used to switch the locator off here, and so never saw that production
                // had it on, with /{service-id}/** routes going around the limiter.
                "spring.cloud.config.enabled=false",
                "eureka.client.enabled=false",
                "management.tracing.enabled=false"
        })
class GatewayRateLimitIT {

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    /** One WireMock serves both downstream services; the stubbed paths never overlap. */
    static final WireMockServer BACKEND = new WireMockServer(WireMockConfiguration.options().dynamicPort());

    static {
        REDIS.start();
        BACKEND.start();
        BACKEND.stubFor(post(urlPathEqualTo("/api/tickets/reserve"))
                .willReturn(okJson("{\"code\":200,\"message\":\"OK\",\"data\":{\"reservationId\":1}}")));
        BACKEND.stubFor(post(urlPathMatching("/api/auctions/\\d+/bids"))
                .willReturn(okJson("{\"code\":200,\"message\":\"OK\",\"data\":null}")));
        BACKEND.stubFor(get(urlPathMatching("/api/tickets/events/\\d+"))
                .willReturn(okJson("{\"code\":200,\"message\":\"OK\",\"data\":[]}")));
        // Lowest priority: anything else that reaches the backend is answered 200 too, so a
        // request that got around the limiter shows up as a success, not as a stub's 404.
        BACKEND.stubFor(any(anyUrl()).atPriority(10)
                .willReturn(okJson("{\"code\":200,\"message\":\"OK\",\"data\":null}")));
    }

    @DynamicPropertySource
    static void wiring(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.cloud.discovery.client.simple.instances.ticket-service[0].uri", BACKEND::baseUrl);
        registry.add("spring.cloud.discovery.client.simple.instances.auction-service[0].uri", BACKEND::baseUrl);
    }

    @Autowired
    WebTestClient webTestClient;

    @LocalServerPort
    int port;

    @BeforeEach
    void resetDownstreamRequestLog() {
        BACKEND.resetRequests();
    }

    @Test
    void reserveRoute_throttlesPerUser_andThrottledRequestsNeverReachTheService() {
        String token = mintToken(101);
        // Warm up the Redis script/connection with a different user so the first
        // requests of the measured burst are not artificially slow.
        postReserve(mintToken(999101));
        BACKEND.resetRequests();

        int successes = 0;
        int throttled = 0;
        String remainingOnThrottle = null;
        for (int i = 0; i < 12; i++) {
            FluxExchangeResult<String> result = postReserve(token);
            if (result.getStatus().value() == 200) {
                successes++;
            } else if (result.getStatus().value() == 429) {
                throttled++;
                remainingOnThrottle = result.getResponseHeaders().getFirst("X-RateLimit-Remaining");
            } else {
                fail("Unexpected status " + result.getStatus() + " on request " + i);
            }
        }

        // burst=3 tokens up front, plus at most a few replenished (1/s) while looping.
        assertThat(successes).as("passed requests").isBetween(3, 9);
        assertThat(throttled).as("throttled requests").isGreaterThanOrEqualTo(3);
        assertThat(remainingOnThrottle).as("X-RateLimit-Remaining on a 429").isEqualTo("0");
        // Rejections happen at the gateway: the backend saw exactly the passed requests.
        assertThat(BACKEND.countRequestsMatching(
                postRequestedFor(urlPathEqualTo("/api/tickets/reserve")).build()).getCount())
                .isEqualTo(successes);
    }

    @Test
    void throttlingOneUser_doesNotAffectAnotherUser() {
        String heavyUser = mintToken(201);
        boolean saw429 = false;
        for (int i = 0; i < 20 && !saw429; i++) {
            saw429 = postReserve(heavyUser).getStatus().value() == 429;
        }
        assertThat(saw429).as("heavy user eventually throttled").isTrue();

        assertThat(postReserve(mintToken(202)).getStatus().value())
                .as("other user's bucket is untouched")
                .isEqualTo(200);
    }

    @Test
    void bidRoute_isThrottledIndependently() {
        String token = mintToken(301);
        int successes = 0;
        int throttled = 0;
        for (int i = 0; i < 12; i++) {
            int status = webTestClient.post().uri("/api/auctions/7/bids")
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{\"amount\":100}")
                    .exchange()
                    .returnResult(String.class)
                    .getStatus().value();
            if (status == 200) {
                successes++;
            } else if (status == 429) {
                throttled++;
            } else {
                fail("Unexpected status " + status + " on request " + i);
            }
        }
        assertThat(successes).isGreaterThanOrEqualTo(3);
        assertThat(throttled).isGreaterThanOrEqualTo(1);
    }

    @Test
    void nonHotRoutes_bypassTheRateLimiter() {
        String token = mintToken(401);
        for (int i = 0; i < 15; i++) {
            webTestClient.get().uri("/api/tickets/events/1")
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                    .exchange()
                    .expectStatus().isOk();
        }
    }

    /**
     * The gateway drops empty path segments when it builds the outgoing URL, after route
     * matching. While a wildcard route existed, /api/tickets//reserve missed the limited
     * route, matched the wildcard, and reached the service as /api/tickets/reserve with no
     * limit at all. Sent over a raw socket: WebTestClient would collapse the "//" itself.
     */
    @Test
    void emptySegmentAlias_cannotGoAroundTheLimiter() {
        Map<String, String> auth = Map.of("Authorization", "Bearer " + mintToken(501));

        for (int i = 0; i < 15; i++) {
            assertThat(RawHttp.send(port, "POST", "/api/tickets//reserve", auth).status()).isEqualTo(400);
            assertThat(RawHttp.send(port, "POST", "/api/auctions//7/bids", auth).status()).isEqualTo(400);
            assertThat(RawHttp.send(port, "POST", "/api/auctions/7//bids", auth).status()).isEqualTo(400);
        }

        assertThat(BACKEND.countRequestsMatching(anyRequestedFor(anyUrl()).build()).getCount())
                .as("requests that reached a service around the limiter")
                .isZero();
    }

    /**
     * With the discovery locator on, every registered service is also published as
     * /{service-id}/**, on a route that carries no rate limiter.
     */
    @Test
    void discoveryLocatorPath_cannotGoAroundTheLimiter() {
        Map<String, String> auth = Map.of("Authorization", "Bearer " + mintToken(502));

        for (int i = 0; i < 15; i++) {
            assertThat(RawHttp.send(port, "POST", "/ticket-service/api/tickets/reserve", auth).status())
                    .isEqualTo(404);
            assertThat(RawHttp.send(port, "POST", "/auction-service/api/auctions/7/bids", auth).status())
                    .isEqualTo(404);
        }

        assertThat(BACKEND.countRequestsMatching(anyRequestedFor(anyUrl()).build()).getCount())
                .as("requests that reached a service around the limiter")
                .isZero();
    }

    /**
     * Only ticket-service and auction-service have an instance in this test, so order-service
     * is a listed route whose service is down. That is an outage and reads as one (503),
     * while a path that is simply not listed is 404. The two used to be indistinguishable.
     */
    @Test
    void listedRouteWhoseServiceIsDown_is503_whileAnUnlistedPathIs404() {
        Map<String, String> auth = Map.of("Authorization", "Bearer " + mintToken(503));

        RawHttp.Response serviceDown = RawHttp.send(port, "GET", "/api/orders", auth);
        RawHttp.Response notListed = RawHttp.send(port, "GET", "/api/orders/9/refund", auth);

        assertThat(serviceDown.status()).isEqualTo(503);
        assertThat(serviceDown.body()).contains("Service temporarily unavailable");
        assertThat(notListed.status()).isEqualTo(404);
        assertThat(BACKEND.countRequestsMatching(anyRequestedFor(anyUrl()).build()).getCount()).isZero();
    }

    private FluxExchangeResult<String> postReserve(String token) {
        return webTestClient.post().uri("/api/tickets/reserve")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"eventId\":1,\"ticketType\":\"VIP\",\"quantity\":1}")
                .exchange()
                .returnResult(String.class);
    }

    private static String mintToken(long userId) {
        return TestTokens.mint(userId);
    }
}
