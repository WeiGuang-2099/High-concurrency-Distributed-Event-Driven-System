package com.auction.auction.security;

import com.auction.common.security.UserContext;

public final class UserContextHolder {

    private static final ThreadLocal<UserContext> CONTEXT = new ThreadLocal<>();

    public static void set(UserContext ctx) {
        CONTEXT.set(ctx);
    }

    public static UserContext get() {
        return CONTEXT.get();
    }

    public static void clear() {
        CONTEXT.remove();
    }

    private UserContextHolder() {
    }
}
