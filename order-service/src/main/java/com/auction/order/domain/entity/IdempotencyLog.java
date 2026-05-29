package com.auction.order.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("idempotency_log")
public class IdempotencyLog {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String idempotencyKey;

    private String eventType;

    private LocalDateTime processedAt;
}
