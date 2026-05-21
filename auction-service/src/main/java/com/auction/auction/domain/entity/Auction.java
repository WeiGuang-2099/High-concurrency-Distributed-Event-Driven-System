package com.auction.auction.domain.entity;

import com.auction.auction.domain.enums.AuctionStatus;
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
@TableName("auction")
public class Auction {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String eventName;

    private String description;

    private Long ticketTypeId;

    private BigDecimal startingPrice;

    private BigDecimal currentHighestBid;

    private Long currentHighestBidderId;

    private AuctionStatus status;

    private LocalDateTime startTime;

    private LocalDateTime endTime;

    private Long winnerId;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
