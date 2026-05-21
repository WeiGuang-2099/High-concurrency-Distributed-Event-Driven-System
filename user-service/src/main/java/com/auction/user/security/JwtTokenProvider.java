package com.auction.user.security;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.security.KeyStore;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

@Component
public class JwtTokenProvider {

    private static final Logger log = LoggerFactory.getLogger(JwtTokenProvider.class);

    @Value("${jwt.keystore.path}")
    private String keystorePath;

    @Value("${jwt.keystore.password}")
    private String keystorePassword;

    @Value("${jwt.keystore.alias}")
    private String keystoreAlias;

    @Value("${jwt.issuer}")
    private String issuer;

    @Value("${jwt.access-token-ttl-seconds}")
    private long accessTokenTtlSeconds;

    private RSAPrivateKey privateKey;
    private RSAPublicKey publicKey;

    @PostConstruct
    public void init() {
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(
                    keystorePath.replace("classpath:", ""))) {
                if (is == null) {
                    throw new RuntimeException("Keystore not found: " + keystorePath);
                }
                keyStore.load(is, keystorePassword.toCharArray());
            }
            privateKey = (RSAPrivateKey) keyStore.getKey(keystoreAlias, keystorePassword.toCharArray());
            publicKey = (RSAPublicKey) keyStore.getCertificate(keystoreAlias).getPublicKey();
            log.info("RSA key pair loaded successfully from keystore");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load RSA keystore", e);
        }
    }

    public String createAccessToken(Long userId, String username, Set<String> roles) {
        try {
            Instant now = Instant.now();
            Instant expiry = now.plusSeconds(accessTokenTtlSeconds);
            String jti = UUID.randomUUID().toString().replace("-", "");

            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .subject(String.valueOf(userId))
                    .claim("username", username)
                    .claim("roles", String.join(",", roles))
                    .claim("jti", jti)
                    .issuer(issuer)
                    .issueTime(Date.from(now))
                    .expirationTime(Date.from(expiry))
                    .build();

            SignedJWT signedJWT = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256)
                            .type(JOSEObjectType.JWT)
                            .build(),
                    claims);

            signedJWT.sign(new RSASSASigner(privateKey));
            return signedJWT.serialize();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create JWT token", e);
        }
    }

    public UserPrincipal parseToken(String token) {
        try {
            SignedJWT signedJWT = SignedJWT.parse(token);

            if (!signedJWT.verify(new RSASSAVerifier(publicKey))) {
                return null;
            }

            JWTClaimsSet claims = signedJWT.getJWTClaimsSet();

            if (claims.getExpirationTime() != null &&
                    claims.getExpirationTime().toInstant().isBefore(Instant.now())) {
                return null;
            }

            if (!issuer.equals(claims.getIssuer())) {
                return null;
            }

            String rolesStr = claims.getStringClaim("roles");
            Set<String> roles = Set.of(rolesStr.split(","));

            return UserPrincipal.builder()
                    .userId(Long.parseLong(claims.getSubject()))
                    .username(claims.getStringClaim("username"))
                    .roles(roles)
                    .jti(claims.getStringClaim("jti"))
                    .expiry(claims.getExpirationTime().toInstant())
                    .build();
        } catch (Exception e) {
            log.debug("Failed to parse JWT token: {}", e.getMessage());
            return null;
        }
    }

    public long getAccessTokenTtlSeconds() {
        return accessTokenTtlSeconds;
    }
}
