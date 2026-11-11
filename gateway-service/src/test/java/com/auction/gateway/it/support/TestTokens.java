package com.auction.gateway.it.support;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.interfaces.RSAPrivateKey;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

/**
 * Mints JWTs with the development keystore user-service signs with, carrying the same
 * claims its JwtTokenProvider issues, so the gateway's real verification path runs.
 */
public final class TestTokens {

    private static final RSAPrivateKey PRIVATE_KEY = loadPrivateKey();

    private TestTokens() {
    }

    public static String mint(long userId) {
        return mint(userId, "USER");
    }

    public static String mint(long userId, String roles) {
        try {
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .subject(String.valueOf(userId))
                    .claim("username", "u" + userId)
                    .claim("roles", roles)
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
     * Failsafe runs with the module directory as working directory; files that live in
     * another module are found by walking up to the repository root first.
     */
    public static Path repoFile(String relative) {
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
