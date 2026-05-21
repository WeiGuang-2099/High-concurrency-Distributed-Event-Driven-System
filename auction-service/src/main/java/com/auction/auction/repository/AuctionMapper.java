package com.auction.auction.repository;

import com.auction.auction.domain.entity.Auction;
import com.auction.auction.domain.enums.AuctionStatus;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

@Mapper
public interface AuctionMapper extends BaseMapper<Auction> {

    @Select("SELECT * FROM auction WHERE status = #{status} AND start_time <= #{now} LIMIT 100")
    List<Auction> findReadyToActivate(@Param("status") String status, @Param("now") LocalDateTime now);

    @Select("SELECT * FROM auction WHERE status = #{status} AND end_time <= #{now} LIMIT 100")
    List<Auction> findReadyToExpire(@Param("status") String status, @Param("now") LocalDateTime now);

    @Update("UPDATE auction SET status = #{newStatus} WHERE id = #{id} AND status = #{expectedStatus}")
    int compareAndSetStatus(@Param("id") Long id,
                            @Param("expectedStatus") String expectedStatus,
                            @Param("newStatus") String newStatus);

    @Update("UPDATE auction SET status = 'SETTLED', winner_id = #{winnerId}, current_highest_bid = #{finalAmount} " +
            "WHERE id = #{id} AND status = 'ACTIVE'")
    int markSettled(@Param("id") Long id,
                    @Param("winnerId") Long winnerId,
                    @Param("finalAmount") java.math.BigDecimal finalAmount);

    default int markActive(Long id) {
        return compareAndSetStatus(id, AuctionStatus.PENDING.getValue(), AuctionStatus.ACTIVE.getValue());
    }

    default int markExpired(Long id) {
        return compareAndSetStatus(id, AuctionStatus.ACTIVE.getValue(), AuctionStatus.EXPIRED.getValue());
    }
}
