package com.auction.user.security;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserPrincipal {

    private Long userId;
    private String username;
    private Set<String> roles;
    private String jti;
    private Instant expiry;
}
