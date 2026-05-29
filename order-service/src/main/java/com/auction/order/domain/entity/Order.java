package com.auction.order.domain.entity;

import com.auction.order.domain.enums.OrderStatus;
import com.auction.order.domain.enums.OrderType;
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
@TableName("orders")
public class Order {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long userId;

    private OrderType type;

    private Long referenceId;

    private BigDecimal amount;

    private OrderStatus status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    private LocalDateTime paidAt;
}
