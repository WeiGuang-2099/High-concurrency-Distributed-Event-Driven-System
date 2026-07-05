package com.auction.gateway.it;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.interfaces.RSAPrivateKey;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

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
                // Keep the test hermetic: no config-server fetch, no Eureka, no
                // discovery-locator routes on top of the declared ones.
                "spring.cloud.config.enabled=false",
                "eureka.client.enabled=false",
                "spring.cloud.gateway.discovery.locator.enabled=false",
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

    private FluxExchangeResult<String> postReserve(String token) {
        return webTestClient.post().uri("/api/tickets/reserve")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"eventId\":1,\"ticketType\":\"VIP\",\"quantity\":1}")
                .exchange()
                .returnResult(String.class);
    }

    // --- JWT minting against the real user-service keystore -------------------------

    private static final RSAPrivateKey PRIVATE_KEY = loadPrivateKey();

    /** Same claims user-service's JwtTokenProvider issues, so the gateway accepts it. */
    private static String mintToken(long userId) {
        try {
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .subject(String.valueOf(userId))
                    .claim("username", "u" + userId)
                    .claim("roles", "USER")
                    .claim("jti", UUID.randomUUID().toString().replace("-", ""))
                    .issuer("auction-platform")
                    .issueTime(new Date())
                    .expirationTime(Date.from(Instant.now().plusSeconds(3600)))
                    .build();
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).build(), claims);
            jwt.sign(new RSASSASigner(PRIVATE_KEY));
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to mint test JWT", e);
        }
    }

    private static RSAPrivateKey loadPrivateKey() {
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (InputStream is = Files.newInputStream(
                    repoFile("user-service/src/main/resources/keys/jwt-keystore.p12"))) {
                keyStore.load(is, "auction2024".toCharArray());
            }
            return (RSAPrivateKey) keyStore.getKey("jwt-key", "auction2024".toCharArray());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load user-service JWT keystore", e);
        }
    }

    /**
     * Failsafe runs with the module directory as working directory; the keystore lives
     * in the user-service module, so walk upwards to the repo root first.
     */
    private static Path repoFile(String relative) {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve(relative);
            if (Files.exists(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Cannot locate " + relative + " above " + Paths.get("").toAbsolutePath());
    }
}
