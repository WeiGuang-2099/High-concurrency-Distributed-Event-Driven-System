package com.auction.auction.domain.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("bid")
public class Bid {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long auctionId;

    private Long userId;

    private String username;

    private BigDecimal amount;

    private LocalDateTime bidTime;

    private String eventId;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;
}
