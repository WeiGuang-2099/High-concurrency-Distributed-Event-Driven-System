package com.auction.order.controller.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PayResponse {

    private Long orderId;
    private String status;
    private String paymentStatus;
    private String message;
}
