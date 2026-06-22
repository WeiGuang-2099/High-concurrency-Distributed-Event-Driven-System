package com.auction.common.feign;

import com.auction.common.security.GatewayHeaders;
import com.auction.common.security.UserContext;
import com.auction.common.security.UserContextHolder;
import feign.RequestInterceptor;
import feign.RequestTemplate;

/**
 * Feign interceptor that propagates the gateway-injected user identity headers
 * (X-User-Id, X-Username, X-User-Roles) from the current request context to
 * downstream service calls. Without this, inter-service Feign calls would lose
 * the user context and the downstream UserContextFilter would set nothing,
 * causing UserContextHolder.get() to return null.
 *
 * <p>Register as a @Bean in any service that makes Feign calls requiring user context.
 */
public class UserContextFeignInterceptor implements RequestInterceptor {

    @Override
    public void apply(RequestTemplate template) {
        UserContext ctx = UserContextHolder.get();
        if (ctx != null) {
            if (ctx.getUserId() != null) {
                template.header(GatewayHeaders.USER_ID, String.valueOf(ctx.getUserId()));
            }
            if (ctx.getUsername() != null) {
                template.header(GatewayHeaders.USERNAME, ctx.getUsername());
            }
            if (ctx.getRoles() != null && !ctx.getRoles().isEmpty()) {
                template.header(GatewayHeaders.USER_ROLES, String.join(",", ctx.getRoles()));
            }
        }
    }
}