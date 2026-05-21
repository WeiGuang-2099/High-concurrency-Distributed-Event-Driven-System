package com.auction.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {

    private int code;
    private String message;
    private Instant timestamp;
    private String traceId;

    public static ErrorResponse of(int code, String message, String traceId) {
        return new ErrorResponse(code, message, Instant.now(), traceId);
    }
}
