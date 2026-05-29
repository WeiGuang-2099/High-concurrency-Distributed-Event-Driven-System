package com.auction.order.repository;

import com.auction.order.domain.entity.IdempotencyLog;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface IdempotencyLogMapper extends BaseMapper<IdempotencyLog> {

    @Select("SELECT COUNT(*) FROM idempotency_log WHERE idempotency_key = #{key}")
    int existsByKey(@Param("key") String key);
}
