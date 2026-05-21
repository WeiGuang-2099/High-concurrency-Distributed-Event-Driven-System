package com.auction.notification.config;

import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;

@Component
public class WebSocketAuthInterceptor implements ChannelInterceptor {

    private static final Logger log = LoggerFactory.getLogger(WebSocketAuthInterceptor.class);

    private final JwtProperties jwtProperties;
    private RSASSAVerifier verifier;

    public WebSocketAuthInterceptor(JwtProperties jwtProperties) {
        this.jwtProperties = jwtProperties;
    }

    @PostConstruct
    public void init() {
        try {
            String publicKeyPem = jwtProperties.getPublicKey();
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate cert = (X509Certificate) cf.generateCertificate(
                    new ByteArrayInputStream(publicKeyPem.getBytes()));
            RSAPublicKey publicKey = (RSAPublicKey) cert.getPublicKey();
            this.verifier = new RSASSAVerifier(publicKey);
            log.info("Notification-service JWT public key loaded for WebSocket auth");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load JWT public key for WebSocket auth", e);
        }
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);

        if (accessor != null && StompCommand.CONNECT.equals(accessor.getCommand())) {
            // Extract JWT from native headers (STOMP CONNECT frame)
            String token = accessor.getFirstNativeHeader("Authorization");
            if (token == null || !token.startsWith("Bearer ")) {
                log.warn("WebSocket CONNECT rejected: missing or invalid Authorization header");
                throw new IllegalArgumentException("Missing or invalid Authorization header");
            }

            token = token.substring(7);

            try {
                SignedJWT signedJWT = SignedJWT.parse(token);

                // Verify signature
                if (!signedJWT.verify(verifier)) {
                    log.warn("WebSocket CONNECT rejected: invalid token signature");
                    throw new IllegalArgumentException("Invalid token signature");
                }

                // Verify issuer
                String issuer = signedJWT.getJWTClaimsSet().getIssuer();
                if (!jwtProperties.getIssuer().equals(issuer)) {
                    log.warn("WebSocket CONNECT rejected: invalid issuer");
                    throw new IllegalArgumentException("Invalid token issuer");
                }

                // Verify expiration
                if (signedJWT.getJWTClaimsSet().getExpirationTime() != null &&
                        signedJWT.getJWTClaimsSet().getExpirationTime().getTime() < System.currentTimeMillis()) {
                    log.warn("WebSocket CONNECT rejected: token expired");
                    throw new IllegalArgumentException("Token expired");
                }

                // Store user info in session attributes for later use
                String userId = signedJWT.getJWTClaimsSet().getSubject();
                String username = signedJWT.getJWTClaimsSet().getStringClaim("username");
                accessor.getSessionAttributes().put("userId", userId);
                accessor.getSessionAttributes().put("username", username);

                log.debug("WebSocket CONNECT authenticated: userId={}", userId);

            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                log.warn("WebSocket CONNECT rejected: token parsing error", e);
                throw new IllegalArgumentException("Invalid token");
            }
        }

        return message;
    }
}
