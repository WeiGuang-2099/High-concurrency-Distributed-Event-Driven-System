package com.auction.ticket.repository;

import com.auction.ticket.domain.entity.Reservation;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface ReservationMapper extends BaseMapper<Reservation> {

    @Select("SELECT * FROM reservation WHERE id = #{id} AND user_id = #{userId}")
    Reservation findByIdAndUserId(@Param("id") Long id, @Param("userId") Long userId);

    @Select("SELECT * FROM reservation WHERE id = #{id}")
    Reservation findById(@Param("id") Long id);

    @Update("UPDATE reservation SET status = #{status} WHERE id = #{id} AND status = 'PENDING'")
    int updateStatusIfPending(@Param("id") Long id, @Param("status") String status);
}
