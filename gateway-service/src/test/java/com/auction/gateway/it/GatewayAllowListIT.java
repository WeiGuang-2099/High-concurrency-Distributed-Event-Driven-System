package com.auction.gateway.it;

import com.auction.gateway.it.support.RawHttp;
import com.auction.gateway.it.support.RecordingBackend;
import com.auction.gateway.it.support.TestTokens;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteDefinitionLocator;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.netty.http.client.HttpClient;

import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gateway is an allow-list: a request reaches a service only if its method and exact
 * path are listed in application.yml, and only if the path is written in canonical form.
 *
 * <p>Boots the real gateway on the shipped configuration. The only properties replaced are
 * the ones a hermetic test cannot keep: the config server, Eureka (the simple discovery
 * client stands in, with one recording backend per service id), and the Redis address.
 * Routes, filters, CORS and the discovery-locator setting are the production ones, so a
 * change there is a change here. Requests go out over a plain socket because the usual
 * test clients normalise the path before sending it.
 *
 * <p>The config server is off, but what it would serve is not left out: the two files it
 * hands the gateway are imported from config-repo/ with the same precedence, above
 * application.yml. In a deployment those values win, so a routing key added there (the
 * discovery locator switched back on, say) would change the route table while a test that
 * only read application.yml stayed green.
 *
 * <p>"Not forwarded" is asserted on what the backends received, never on the status code
 * alone: every backend answers 200 to anything, so a leaked request cannot hide behind a
 * stub's 404.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.cloud.config.enabled=false",
                "eureka.client.enabled=false",
                // Relative to the module directory, which is where Maven runs the tests.
                // Not "optional:": a wrong path must fail the test, not skip the files.
                "spring.config.import=file:../config-repo/application.yml,file:../config-repo/gateway-service.yml"
        })
class GatewayAllowListIT {

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    /** Every service that registers with Eureka in a real deployment, gateway aside. */
    static final List<RecordingBackend> BACKENDS = List.of(
            new RecordingBackend("user-service"),
            new RecordingBackend("auction-service"),
            new RecordingBackend("ticket-service"),
            new RecordingBackend("order-service"),
            new RecordingBackend("notification-service"),
            new RecordingBackend("event-store-service"));

    static {
        REDIS.start();
    }

    @DynamicPropertySource
    static void wiring(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        for (RecordingBackend backend : BACKENDS) {
            registry.add("spring.cloud.discovery.client.simple.instances." + backend.name() + "[0].uri",
                    backend::baseUrl);
        }
    }

    private static final AtomicLong NEXT_USER_ID = new AtomicLong(700_000);

    @LocalServerPort
    int port;

    @Autowired
    RouteDefinitionLocator routeDefinitionLocator;

    @Autowired
    RouteLocator routeLocator;

    @Autowired
    ReactiveStringRedisTemplate redis;

    @BeforeEach
    void resetBackends() {
        BACKENDS.forEach(RecordingBackend::reset);
    }

    // --- the list itself --------------------------------------------------------------

    /**
     * A second copy of the allow-list, on purpose. Adding, widening or removing a route in
     * application.yml fails this test until the same change is made here, so the public
     * surface cannot change as a side effect of something else.
     *
     * <p>A line also carries the route's filters. The limiter appears with the rates that
     * ship (replenish per second / burst), which no other test runs on: GatewayRateLimitIT
     * tightens them to make the bucket empty quickly. Any other filter appears by name, so a
     * RewritePath or SetPath that sent a listed path to an unlisted handler would show here.
     */
    @Test
    void routeTable_isExactlyTheAllowList() {
        assertThat(describeRouteTable()).containsExactlyInAnyOrder(
                "POST /api/users/register -> user-service public",
                "POST /api/users/login -> user-service public",
                "POST /api/users/logout -> user-service",
                "GET /api/users/me -> user-service",
                "PUT /api/users/me -> user-service",

                "GET /api/auctions -> auction-service",
                "GET /api/auctions/hot -> auction-service",
                "GET /api/auctions/{id:[0-9]+} -> auction-service",
                "GET /api/auctions/{auctionId:[0-9]+}/bids -> auction-service",
                "POST /api/auctions/{auctionId:[0-9]+}/bids -> auction-service rate-limited 10/20 by #{@userKeyResolver}",
                "POST /api/admin/auctions -> auction-service",

                "GET /api/tickets/events/{eventId:[0-9]+} -> ticket-service",
                "POST /api/tickets/reserve -> ticket-service rate-limited 5/10 by #{@userKeyResolver}",
                "POST /api/admin/tickets -> ticket-service",

                "POST /api/orders -> order-service",
                "GET /api/orders -> order-service",
                "GET /api/orders/{id:[0-9]+} -> order-service",
                "POST /api/orders/{id:[0-9]+}/pay -> order-service",
                "POST /api/orders/{id:[0-9]+}/cancel -> order-service",

                "GET /api/notifications -> notification-service",
                "PUT /api/notifications/{id}/read -> notification-service",

                "GET /ws/notifications/info -> notification-service public",
                "GET /ws/notifications/{server}/{session}/websocket -> notification-service public",
                "GET /ws/notifications/{server}/{session}/eventsource -> notification-service public",
                "POST /ws/notifications/{server}/{session}/xhr -> notification-service public",
                "POST /ws/notifications/{server}/{session}/xhr_send -> notification-service public",
                "POST /ws/notifications/{server}/{session}/xhr_streaming -> notification-service public");
    }

    @Test
    void routeTable_hasNoWildcards_andNoDiscoveryRoutes() {
        List<RouteDefinition> routes = routeDefinitions();

        assertThat(routes).isNotEmpty();
        assertThat(routes).extracting(RouteDefinition::getId)
                .as("route ids")
                .noneMatch(id -> id.startsWith("ReactiveCompositeDiscoveryClient_"));
        for (RouteDefinition route : routes) {
            assertThat(argsOf(route, "Path")).as("Path patterns of route %s", route.getId())
                    .isNotEmpty()
                    .noneMatch(pattern -> pattern.contains("*"));
            assertThat(argsOf(route, "Method")).as("Method predicate of route %s", route.getId())
                    .isNotEmpty();
        }
    }

    /**
     * The two tests above read route definitions. This one reads what the gateway actually
     * serves, which also includes any route built in Java by a RouteLocator bean.
     */
    @Test
    void routesTheGatewayServes_areExactlyTheDefinedOnes() {
        List<String> served = routeLocator.getRoutes().map(Route::getId).collectList().block(Duration.ofSeconds(10));
        List<String> defined = routeDefinitions().stream().map(RouteDefinition::getId).toList();

        assertThat(served).containsExactlyInAnyOrderElementsOf(defined);
    }

    // --- listed endpoints reach their own service ---------------------------------------

    record Endpoint(String method, String path, String service, boolean isPublic) {
    }

    static Stream<Endpoint> allowListed() {
        return Stream.of(
                new Endpoint("POST", "/api/users/register", "user-service", true),
                new Endpoint("POST", "/api/users/login", "user-service", true),
                new Endpoint("POST", "/api/users/logout", "user-service", false),
                new Endpoint("GET", "/api/users/me", "user-service", false),
                new Endpoint("PUT", "/api/users/me", "user-service", false),

                new Endpoint("GET", "/api/auctions", "auction-service", false),
                new Endpoint("GET", "/api/auctions?page=0&size=20", "auction-service", false),
                new Endpoint("GET", "/api/auctions/hot", "auction-service", false),
                new Endpoint("GET", "/api/auctions/42", "auction-service", false),
                new Endpoint("GET", "/api/auctions/42/bids", "auction-service", false),
                new Endpoint("POST", "/api/auctions/42/bids", "auction-service", false),
                new Endpoint("POST", "/api/admin/auctions", "auction-service", false),

                new Endpoint("GET", "/api/tickets/events/7", "ticket-service", false),
                new Endpoint("POST", "/api/tickets/reserve", "ticket-service", false),
                new Endpoint("POST", "/api/admin/tickets", "ticket-service", false),

                new Endpoint("POST", "/api/orders", "order-service", false),
                new Endpoint("GET", "/api/orders", "order-service", false),
                new Endpoint("GET", "/api/orders/9", "order-service", false),
                new Endpoint("POST", "/api/orders/9/pay", "order-service", false),
                new Endpoint("POST", "/api/orders/9/cancel", "order-service", false),

                new Endpoint("GET", "/api/notifications", "notification-service", false),
                new Endpoint("PUT", "/api/notifications/65f1c0ffee65f1c0ffee65f1/read", "notification-service", false),

                // SockJS: the browser cannot attach an Authorization header to these, so they
                // are public at the HTTP layer; notification-service checks the JWT on the
                // STOMP CONNECT frame.
                new Endpoint("GET", "/ws/notifications/info?t=1731400000000", "notification-service", true),
                new Endpoint("GET", "/ws/notifications/123/abcd0123/websocket", "notification-service", true),
                new Endpoint("GET", "/ws/notifications/123/abcd0123/eventsource", "notification-service", true),
                new Endpoint("POST", "/ws/notifications/123/abcd0123/xhr", "notification-service", true),
                new Endpoint("POST", "/ws/notifications/123/abcd0123/xhr_send", "notification-service", true),
                new Endpoint("POST", "/ws/notifications/123/abcd0123/xhr_streaming", "notification-service", true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allowListed")
    void listedEndpoint_withToken_reachesItsOwnServiceUnchanged(Endpoint endpoint) {
        RawHttp.Response response = send(endpoint.method(), endpoint.path(), freshToken());

        assertThat(response.status()).as("status of %s %s", endpoint.method(), endpoint.path()).isEqualTo(200);
        assertThat(response.header("X-Backend")).isEqualTo(endpoint.service());
        assertThat(forwarded()).containsExactly(
                endpoint.service() + " <- " + endpoint.method() + " " + endpoint.path());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allowListed")
    void listedEndpoint_withoutToken_isServedOnlyIfPublic(Endpoint endpoint) {
        RawHttp.Response response = send(endpoint.method(), endpoint.path(), null);

        if (endpoint.isPublic()) {
            assertThat(response.status()).isEqualTo(200);
            assertThat(forwarded()).containsExactly(
                    endpoint.service() + " <- " + endpoint.method() + " " + endpoint.path());
        } else {
            assertThat(response.status()).isEqualTo(401);
            assertThat(forwarded()).as("requests forwarded without a token").isEmpty();
        }
    }

    // --- everything else is not routed --------------------------------------------------

    static Stream<Arguments> unlisted() {
        return Stream.of(
                // Confirm is called by order-service after payment, service to service. A client
                // calling it turned its own pending reservation into a sold ticket for free.
                Arguments.of("POST", "/api/tickets/1/confirm"),
                // Benchmark-only endpoint; k6 calls it on the service port.
                Arguments.of("POST", "/api/tickets/reserve-pessimistic"),
                // A reservation is released through its order: POST /api/orders/{id}/cancel.
                Arguments.of("DELETE", "/api/tickets/1"),
                // event-store-service has no authorisation of its own.
                Arguments.of("GET", "/api/event-store/auctions/1/history"),
                Arguments.of("GET", "/api/event-store/orders/1/timeline"),
                Arguments.of("GET", "/api/event-store/stock/1/movements"),
                Arguments.of("POST", "/api/admin/events/replay/Auction/1"),

                // What the discovery locator used to publish for every registered service,
                // around the rate limiters and around this list.
                Arguments.of("POST", "/ticket-service/api/tickets/reserve"),
                Arguments.of("POST", "/auction-service/api/auctions/42/bids"),
                Arguments.of("POST", "/ticket-service/api/tickets/1/confirm"),
                Arguments.of("POST", "/auction-service/api/admin/auctions"),
                Arguments.of("GET", "/event-store-service/api/event-store/orders/1/timeline"),
                Arguments.of("GET", "/ticket-service/actuator/metrics"),
                Arguments.of("GET", "/order-service/api/orders"),
                Arguments.of("GET", "/user-service/api/users/me"),
                Arguments.of("GET", "/notification-service/api/notifications"),

                // Listed path, other method.
                Arguments.of("GET", "/api/tickets/reserve"),
                Arguments.of("GET", "/api/admin/auctions"),
                Arguments.of("DELETE", "/api/orders/9"),
                Arguments.of("PUT", "/api/auctions/42"),
                Arguments.of("POST", "/api/users/me"),
                Arguments.of("POST", "/api/users/register/extra"),

                // Ids are plain decimal digits. Long parsing downstream would also take these.
                Arguments.of("GET", "/api/auctions/abc"),
                Arguments.of("GET", "/api/auctions/0x10"),
                Arguments.of("GET", "/api/auctions/-1"),
                Arguments.of("GET", "/api/orders/1.0"),
                Arguments.of("POST", "/api/orders/pay/pay"),

                // Neighbours nobody listed.
                Arguments.of("GET", "/api/tickets"),
                Arguments.of("GET", "/api/tickets/events"),
                Arguments.of("GET", "/api/auctions/42/unknown"),
                Arguments.of("POST", "/api/orders/9/refund"),
                Arguments.of("GET", "/api/users/1"),
                Arguments.of("GET", "/api/admin/tickets/1"),
                Arguments.of("GET", "/api/admin"),

                // SockJS endpoints the client does not use.
                Arguments.of("GET", "/ws/notifications"),
                Arguments.of("GET", "/ws/notifications/websocket"),
                Arguments.of("GET", "/ws/notifications/iframe.html"),
                Arguments.of("GET", "/ws/notifications/123/abcd0123/htmlfile"),
                Arguments.of("GET", "/ws/notifications/123/abcd0123/jsonp"),
                Arguments.of("POST", "/ws/notifications/123/abcd0123/websocket"),
                Arguments.of("GET", "/ws/other/info"),

                // The gateway's own actuator exposes health and info and nothing else.
                Arguments.of("GET", "/actuator/env"),
                Arguments.of("GET", "/actuator/beans"),
                Arguments.of("GET", "/actuator/heapdump"),
                Arguments.of("GET", "/actuator/metrics"),
                Arguments.of("GET", "/actuator/gateway/routes"),

                Arguments.of("GET", "/"),
                Arguments.of("GET", "/nope"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("unlisted")
    void unlistedEndpoint_is404_andNeverForwarded(String method, String path) {
        RawHttp.Response withToken = send(method, path, freshToken());
        RawHttp.Response withoutToken = send(method, path, null);

        assertThat(forwarded()).as("requests forwarded for %s %s", method, path).isEmpty();
        // 404, not the 503 the gateway used to give for "no route": that reads as an outage.
        assertThat(withToken.status()).as("status with a token").isEqualTo(404);
        assertThat(withoutToken.status()).as("status without a token").isEqualTo(404);
        assertThat(withToken.body()).contains("\"code\":404");
    }

    // --- internal endpoints ----------------------------------------------------------------

    static Stream<Arguments> internalPaths() {
        return Stream.of(
                Arguments.of("POST", "/api/tickets/internal/settle-reserve"),
                Arguments.of("POST", "/api/tickets/internal/settle-release"),
                Arguments.of("POST", "/api/orders/internal/auction"),
                Arguments.of("GET", "/api/tickets/internal"),
                Arguments.of("GET", "/internal"),
                Arguments.of("POST", "/api/tickets/INTERNAL/settle-reserve"),
                Arguments.of("POST", "/ticket-service/api/tickets/internal/settle-reserve"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("internalPaths")
    void internalEndpoint_is403_andNeverForwarded(String method, String path) {
        RawHttp.Response response = send(method, path, freshToken());

        assertThat(forwarded()).as("requests forwarded for %s %s", method, path).isEmpty();
        assertThat(response.status()).isEqualTo(403);
    }

    // --- non-canonical paths ---------------------------------------------------------------

    static Stream<String> nonCanonicalTargets() {
        return Stream.of(
                // Path parameters. Spring MVC and the gateway's own Path predicate both drop
                // ";..." before matching, so each of these names a real handler downstream.
                "/api/tickets/internal;x/settle-reserve",
                "/api/tickets/internal;/settle-reserve",
                "/api/tickets/internal;x=1;y=2/settle-reserve",
                "/api/tickets;x/internal/settle-reserve",
                "/api/tickets/internal/settle-reserve;x",
                "/api/tickets/internal;%2F/settle-reserve",
                "/api/tickets/internal;../settle-reserve",
                "/api/orders/internal;x/auction",
                "/api/tickets/reserve;x=1",
                "/api/tickets/1;x/confirm",
                "/api/users/login;.css",
                "http://evil.example/api/tickets/internal;x/settle-reserve",

                // Percent-encoding. Both sides decode a segment before matching it.
                "/api/tickets/%69nternal/settle-reserve",
                "/api/tickets/internal%3Bx/settle-reserve",
                "/api/tickets/internal%2Fsettle-reserve",
                "/api/tickets/internal%00/settle-reserve",
                "/api/tickets/%2569nternal/settle-reserve",
                "/api/tickets/%72eserve",
                "/api/tickets/%31/confirm",
                "/%61pi/tickets/reserve",

                // Empty segments. The gateway collapses "//" when it builds the outgoing URL,
                // after route matching: /api/tickets//reserve used to miss the rate-limited
                // route, match the wildcard one, and arrive downstream as /api/tickets/reserve.
                "/api/tickets//reserve",
                "//api/tickets/reserve",
                "/api/auctions//42/bids",
                "/api/auctions/42//bids",
                "/api/tickets/reserve/",
                "/api/auctions/42/",

                // Dot segments.
                "/api/tickets/./reserve",
                "/api/tickets/x/../reserve",
                "/api/admin/tickets/../../tickets/1/confirm",
                "/ws/../api/notifications",
                "/ws/..;/api/notifications",
                "/ws/notifications/../../api/notifications",

                // Characters no listed path needs.
                "/api/tickets/+1/confirm",
                "/api/auctions/42!",
                "/api/auctions/42,43",
                "/api/tickets/reserve%20",

                // Refused by the HTTP server before any filter runs; listed so the outcome
                // is pinned either way.
                "/api/tickets/internal\\settle-reserve",
                "/api/tickets/internal%zz/settle-reserve",

                // The gateway's own endpoints get the same treatment.
                "/actuator/health;x",
                "/actuator//health");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonCanonicalTargets")
    void nonCanonicalPath_is400_andNeverForwarded(String target) {
        // With a valid token, so the refusal cannot be mistaken for an authentication failure.
        RawHttp.Response post = send("POST", target, freshToken());
        RawHttp.Response get = send("GET", target, freshToken());

        assertThat(forwarded()).as("requests forwarded for %s", target).isEmpty();
        assertThat(post.status()).as("POST %s", target).isEqualTo(400);
        assertThat(get.status()).as("GET %s", target).isEqualTo(400);
    }

    @Test
    void queryString_isNotSubjectToThePathRules() {
        String target = "/api/auctions?page=0&size=20&q=a%20b;c/../d//e";

        RawHttp.Response response = send("GET", target, freshToken());

        assertThat(response.status()).isEqualTo(200);
        assertThat(forwarded()).containsExactly("auction-service <- GET " + target);
    }

    // --- identity headers ------------------------------------------------------------------

    @Test
    void publicRoute_dropsIdentityHeadersSentByTheClient() {
        Map<String, String> forged = Map.of(
                "X-User-Id", "1",
                "X-Username", "admin",
                "X-User-Roles", "ADMIN");

        RawHttp.send(port, "POST", "/api/users/login", forged);
        RawHttp.send(port, "GET", "/ws/notifications/info", forged);

        List<RecordingBackend.Received> received = new ArrayList<>();
        BACKENDS.forEach(backend -> received.addAll(backend.received()));
        assertThat(received).hasSize(2);
        for (RecordingBackend.Received request : received) {
            assertThat(request.headers().get("X-User-Id")).as("X-User-Id on %s", request.uri()).isNull();
            assertThat(request.headers().get("X-Username")).as("X-Username on %s", request.uri()).isNull();
            assertThat(request.headers().get("X-User-Roles")).as("X-User-Roles on %s", request.uri()).isNull();
        }
    }

    @Test
    void authenticatedRoute_replacesIdentityHeadersWithTheTokenClaims() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Bearer " + TestTokens.mint(4242, "USER"));
        headers.put("X-User-Id", "1");
        headers.put("X-Username", "admin");
        headers.put("X-User-Roles", "ADMIN");

        RawHttp.send(port, "GET", "/api/orders", headers);

        RecordingBackend.Received request = backend("order-service").received().get(0);
        assertThat(request.headers().getAll("X-User-Id")).containsExactly("4242");
        assertThat(request.headers().getAll("X-Username")).containsExactly("u4242");
        assertThat(request.headers().getAll("X-User-Roles")).containsExactly("USER");
    }

    // --- token verification --------------------------------------------------------------

    /**
     * The signature, issuer, expiry and revocation checks in JwtAuthenticationFilter. Each
     * token here is refused by a different one of them.
     */
    @Test
    void tokenThatFailsVerification_is401_andNeverForwarded() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        PrivateKey someoneElsesKey = generator.generateKeyPair().getPrivate();
        Instant inAnHour = Instant.now().plusSeconds(3600);

        String valid = TestTokens.mint(880_001);
        String[] parts = valid.split("\\.");
        char flipped = parts[2].charAt(10) == 'A' ? 'B' : 'A';
        String revoked = TestTokens.mint(880_002);
        redis.opsForValue().set("token:blacklist:" + TestTokens.jtiOf(revoked), "1").block(Duration.ofSeconds(5));

        Map<String, String> refused = new LinkedHashMap<>();
        refused.put("signed with another key",
                TestTokens.sign(someoneElsesKey, 880_003, "ADMIN", "auction-platform", inAnHour));
        refused.put("signature altered",
                parts[0] + "." + parts[1] + "." + parts[2].substring(0, 10) + flipped + parts[2].substring(11));
        refused.put("payload swapped under a valid signature",
                parts[0] + "." + TestTokens.mint(1, "ADMIN").split("\\.")[1] + "." + parts[2]);
        refused.put("expired",
                TestTokens.sign(TestTokens.signingKey(), 880_004, "USER", "auction-platform",
                        Instant.now().minusSeconds(60)));
        refused.put("issued by someone else",
                TestTokens.sign(TestTokens.signingKey(), 880_005, "USER", "another-issuer", inAnHour));
        refused.put("revoked at logout", revoked);
        refused.put("not a token", "not-a-jwt");
        refused.put("empty", "");

        for (Map.Entry<String, String> token : refused.entrySet()) {
            BACKENDS.forEach(RecordingBackend::reset);

            RawHttp.Response response = send("GET", "/api/users/me", token.getValue());

            assertThat(forwarded()).as("forwarded with a token that is %s", token.getKey()).isEmpty();
            assertThat(response.status()).as("status for a token that is %s", token.getKey()).isEqualTo(401);
            assertThat(response.body()).contains("\"code\":401");
        }

        // The control: the untouched token the altered ones were made from is accepted.
        assertThat(send("GET", "/api/users/me", valid).status()).isEqualTo(200);
    }

    /**
     * A proxy removes the headers a request names in its Connection header; that is what
     * hop-by-hop means. Left alone, that lets a client with its own valid token name
     * X-User-Id there and have the gateway delete the identity it has just derived from the
     * token, so the service receives an authenticated request with no caller. The same
     * trick removes any other header the service relies on, Authorization included.
     */
    @Test
    void clientCannotHaveForwardedHeadersRemoved_byNamingThemInConnection() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Bearer " + TestTokens.mint(4343, "USER"));
        headers.put("Connection", "X-User-Id, X-Username, x-user-roles, Authorization, close");

        RawHttp.Response response = RawHttp.send(port, "GET", "/api/orders", headers);

        assertThat(response.status()).isEqualTo(200);
        RecordingBackend.Received request = backend("order-service").received().get(0);
        assertThat(request.headers().getAll("X-User-Id")).containsExactly("4343");
        assertThat(request.headers().getAll("X-Username")).containsExactly("u4343");
        assertThat(request.headers().getAll("X-User-Roles")).containsExactly("USER");
        assertThat(request.headers().get("Authorization")).as("Authorization forwarded").startsWith("Bearer ");
    }

    // --- things an allow-list must not break ---------------------------------------------

    /**
     * Every route carries a Method predicate, so an OPTIONS preflight matches none of them.
     * Without globalcors.add-to-simple-url-handler-mapping the browser gets a bare 403 and
     * a cross-origin frontend stops working, which the same-origin dev proxy would never show.
     */
    @Test
    void corsPreflight_isAnsweredByTheGateway() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Origin", "http://localhost:5173");
        headers.put("Access-Control-Request-Method", "POST");
        headers.put("Access-Control-Request-Headers", "authorization,content-type");

        RawHttp.Response response = RawHttp.send(port, "OPTIONS", "/api/tickets/reserve", headers);

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.header("Access-Control-Allow-Origin")).isEqualTo("http://localhost:5173");
        assertThat(response.header("Access-Control-Allow-Methods")).contains("POST");
        assertThat(forwarded()).as("a preflight is answered at the gateway").isEmpty();
    }

    /**
     * A cross-origin page can read the gateway's own 401 and 404 (they carry the CORS
     * headers). The 400 and 403 from the path filters do not: those answer before any CORS
     * processing, on purpose, and nothing a browser sends on its own produces them.
     */
    @Test
    void corsHeaders_onResponsesTheGatewayWritesItself() {
        Map<String, String> origin = Map.of("Origin", "http://localhost:5173");

        RawHttp.Response unauthenticated = RawHttp.send(port, "GET", "/api/users/me", origin);
        RawHttp.Response unlisted = RawHttp.send(port, "GET", "/nope", origin);
        RawHttp.Response nonCanonical = RawHttp.send(port, "GET", "/api/tickets//reserve", origin);
        RawHttp.Response internal = RawHttp.send(port, "GET", "/api/tickets/internal/settle-reserve", origin);

        assertThat(unauthenticated.status()).isEqualTo(401);
        assertThat(unauthenticated.header("Access-Control-Allow-Origin")).isEqualTo("http://localhost:5173");
        assertThat(unlisted.status()).isEqualTo(404);
        assertThat(unlisted.header("Access-Control-Allow-Origin")).isEqualTo("http://localhost:5173");
        assertThat(nonCanonical.status()).isEqualTo(400);
        assertThat(nonCanonical.header("Access-Control-Allow-Origin")).isNull();
        assertThat(internal.status()).isEqualTo(403);
        assertThat(internal.header("Access-Control-Allow-Origin")).isNull();
        assertThat(forwarded()).isEmpty();
    }

    @Test
    void webSocketUpgrade_goesThroughTheListedSockJsRoute() {
        String firstFrame = HttpClient.create()
                .websocket()
                .uri("ws://127.0.0.1:" + port + "/ws/notifications/123/abcd0123/websocket")
                .handle((in, out) -> in.receive().asString().take(1))
                .blockFirst(Duration.ofSeconds(15));

        assertThat(firstFrame).isEqualTo("ws-ok:notification-service");
        assertThat(forwarded()).containsExactly(
                "notification-service <- GET /ws/notifications/123/abcd0123/websocket");
    }

    @Test
    void gatewayHealthEndpoint_staysReachableWithoutAToken() {
        RawHttp.Response response = send("GET", "/actuator/health", null);

        assertThat(response.status()).isEqualTo(200);
        assertThat(forwarded()).isEmpty();
    }

    // --- helpers -----------------------------------------------------------------------------

    private RawHttp.Response send(String method, String target, String token) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (token != null) {
            headers.put("Authorization", "Bearer " + token);
        }
        return RawHttp.send(port, method, target, headers);
    }

    /** A new user per request, so the per-user rate limiter never takes part. */
    private static String freshToken() {
        return TestTokens.mint(NEXT_USER_ID.incrementAndGet());
    }

    private static RecordingBackend backend(String name) {
        return BACKENDS.stream().filter(b -> b.name().equals(name)).findFirst().orElseThrow();
    }

    /** Everything any backend received since the last reset, as "service <- METHOD uri". */
    private static List<String> forwarded() {
        List<String> all = new ArrayList<>();
        for (RecordingBackend backend : BACKENDS) {
            backend.requestLines().forEach(line -> all.add(backend.name() + " <- " + line));
        }
        return all;
    }

    private List<RouteDefinition> routeDefinitions() {
        return routeDefinitionLocator.getRouteDefinitions().collectList().block(Duration.ofSeconds(10));
    }

    /** One line per (method, path pattern) pair: "METHOD pattern -> service [public] [filters]". */
    private List<String> describeRouteTable() {
        List<String> lines = new ArrayList<>();
        for (RouteDefinition route : routeDefinitions()) {
            String suffix = " -> " + route.getUri().getHost();
            if (Boolean.parseBoolean(String.valueOf(route.getMetadata().get("public")))) {
                suffix += " public";
            }
            for (FilterDefinition filter : route.getFilters()) {
                if (filter.getName().equals("RequestRateLimiter")) {
                    suffix += " rate-limited " + filter.getArgs().get("redis-rate-limiter.replenishRate")
                            + "/" + filter.getArgs().get("redis-rate-limiter.burstCapacity")
                            + " by " + filter.getArgs().get("key-resolver");
                } else {
                    suffix += " +" + filter.getName();
                }
            }
            for (String method : argsOf(route, "Method")) {
                for (String pattern : argsOf(route, "Path")) {
                    lines.add(method + " " + pattern + suffix);
                }
            }
        }
        return lines;
    }

    private static List<String> argsOf(RouteDefinition route, String predicateName) {
        List<String> values = new ArrayList<>();
        for (PredicateDefinition predicate : route.getPredicates()) {
            if (predicate.getName().equals(predicateName)) {
                values.addAll(predicate.getArgs().values());
            }
        }
        return values;
    }
}
