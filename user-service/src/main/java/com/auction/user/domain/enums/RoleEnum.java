package com.auction.user.domain.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum RoleEnum {
    USER,
    ADMIN;

    public String getAuthority() {
        return "ROLE_" + name();
    }
}
