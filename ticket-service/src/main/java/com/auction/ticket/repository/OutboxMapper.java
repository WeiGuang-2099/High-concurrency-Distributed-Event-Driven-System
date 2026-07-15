package com.auction.ticket.repository;

import com.auction.ticket.domain.entity.OutboxEvent;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxMapper extends BaseMapper<OutboxEvent> {

    /**
     * Takes ownership of up to {@code limit} unsent rows. Claiming in its own short
     * statement (rather than holding a row lock while publishing) keeps the Kafka round
     * trip out of the transaction -- the mistake ebf22d1 already fixed once on the
     * payment path. A claim older than {@code staleBefore} is retaken, which is how a
     * relay that died mid-publish gets its batch picked up by someone else.
     */
    @Update("UPDATE outbox SET claimed_by = #{claimedBy}, claimed_at = #{now} "
            + "WHERE sent_at IS NULL AND (claimed_by IS NULL OR claimed_at < #{staleBefore}) "
            + "ORDER BY id LIMIT #{limit}")
    int claimUnsent(@Param("claimedBy") String claimedBy,
                    @Param("now") LocalDateTime now,
                    @Param("staleBefore") LocalDateTime staleBefore,
                    @Param("limit") int limit);

    @Select("SELECT * FROM outbox WHERE claimed_by = #{claimedBy} AND sent_at IS NULL ORDER BY id")
    List<OutboxEvent> findClaimed(@Param("claimedBy") String claimedBy);

    @Update("UPDATE outbox SET sent_at = #{now} WHERE id = #{id}")
    int markSent(@Param("id") Long id, @Param("now") LocalDateTime now);
}
