package com.auction.gateway.filter;

import com.auction.common.dto.ErrorResponse;
import com.auction.gateway.config.JwtProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.Set;

@Component
public class JwtAuthenticationFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    private static final Set<String> PUBLIC_PATHS = Set.of(
            "/api/users/register",
            "/api/users/login"
    );

    private final JwtProperties jwtProperties;
    private final ReactiveStringRedisTemplate reactiveStringRedisTemplate;
    private final ObjectMapper objectMapper;

    private RSAPublicKey publicKey;
    private RSASSAVerifier verifier;

    public JwtAuthenticationFilter(JwtProperties jwtProperties,
                                    ReactiveStringRedisTemplate reactiveStringRedisTemplate,
                                    ObjectMapper objectMapper) {
        this.jwtProperties = jwtProperties;
        this.reactiveStringRedisTemplate = reactiveStringRedisTemplate;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        try {
            String publicKeyPem = jwtProperties.getPublicKey();

            // Parse PEM certificate to extract public key
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate cert = (X509Certificate) cf.generateCertificate(
                    new ByteArrayInputStream(publicKeyPem.getBytes()));
            this.publicKey = (RSAPublicKey) cert.getPublicKey();
            this.verifier = new RSASSAVerifier(this.publicKey);

            log.info("Gateway JWT public key loaded successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load JWT public key", e);
        }
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        // Skip auth for public endpoints and actuator
        if (PUBLIC_PATHS.contains(path) || path.startsWith("/actuator")) {
            return chain.filter(exchange);
        }

        // Extract Authorization header
        String authHeader = exchange.getRequest().getHeaders().getFirst("Authorization");
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return writeUnauthorized(exchange, "Missing or invalid Authorization header");
        }

        String token = authHeader.substring(7);

        try {
            SignedJWT signedJWT = SignedJWT.parse(token);

            // Verify signature
            if (!signedJWT.verify(verifier)) {
                return writeUnauthorized(exchange, "Invalid token signature");
            }

            // Verify claims
            String issuer = signedJWT.getJWTClaimsSet().getIssuer();
            if (!jwtProperties.getIssuer().equals(issuer)) {
                return writeUnauthorized(exchange, "Invalid token issuer");
            }

            if (signedJWT.getJWTClaimsSet().getExpirationTime() != null &&
                    signedJWT.getJWTClaimsSet().getExpirationTime().getTime() < System.currentTimeMillis()) {
                return writeUnauthorized(exchange, "Token expired");
            }

            String jti = signedJWT.getJWTClaimsSet().getStringClaim("jti");

            // Check Redis blacklist
            return reactiveStringRedisTemplate.hasKey("token:blacklist:" + jti)
                    .flatMap(blacklisted -> {
                        if (Boolean.TRUE.equals(blacklisted)) {
                            return writeUnauthorized(exchange, "Token revoked");
                        }

                        // Inject user headers
                        try {
                            String userId = signedJWT.getJWTClaimsSet().getSubject();
                            String username = signedJWT.getJWTClaimsSet().getStringClaim("username");
                            String roles = signedJWT.getJWTClaimsSet().getStringClaim("roles");

                            ServerWebExchange mutatedExchange = exchange.mutate()
                                    .request(builder -> builder
                                            .header("X-User-Id", userId)
                                            .header("X-Username", username)
                                            .header("X-User-Roles", roles))
                                    .build();

                            return chain.filter(mutatedExchange);
                        } catch (Exception e) {
                            return writeUnauthorized(exchange, "Invalid token claims");
                        }
                    });

        } catch (Exception e) {
            return writeUnauthorized(exchange, "Invalid token");
        }
    }

    private Mono<Void> writeUnauthorized(ServerWebExchange exchange, String message) {
        String traceId = exchange.getRequest().getHeaders().getFirst("X-Trace-Id");
        ErrorResponse errorResponse = ErrorResponse.of(401, message, traceId);

        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(errorResponse);
        } catch (JsonProcessingException e) {
            bytes = "{\"code\":401,\"message\":\"Unauthorized\"}".getBytes();
        }

        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }
}
