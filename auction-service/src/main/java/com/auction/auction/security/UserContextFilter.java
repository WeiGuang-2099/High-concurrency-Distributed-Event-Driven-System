package com.auction.auction.security;

import com.auction.common.security.GatewayHeaders;
import com.auction.common.security.UserContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Component
public class UserContextFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        try {
            String userIdHeader = request.getHeader(GatewayHeaders.USER_ID);
            if (StringUtils.hasText(userIdHeader)) {
                Set<String> roles = parseRoles(request.getHeader(GatewayHeaders.USER_ROLES));
                UserContext ctx = UserContext.builder()
                        .userId(Long.parseLong(userIdHeader))
                        .username(request.getHeader(GatewayHeaders.USERNAME))
                        .roles(roles)
                        .build();
                UserContextHolder.set(ctx);
            }
            filterChain.doFilter(request, response);
        } finally {
            UserContextHolder.clear();
        }
    }

    private Set<String> parseRoles(String rolesHeader) {
        if (!StringUtils.hasText(rolesHeader)) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(rolesHeader.split(",")));
    }
}
