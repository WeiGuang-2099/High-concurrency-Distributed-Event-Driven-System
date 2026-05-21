package com.auction.auction.domain.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;

public enum AuctionStatus {

    PENDING("PENDING"),
    ACTIVE("ACTIVE"),
    SETTLED("SETTLED"),
    EXPIRED("EXPIRED");

    @EnumValue
    private final String value;

    AuctionStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
