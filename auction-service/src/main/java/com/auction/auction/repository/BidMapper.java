package com.auction.auction.repository;

import com.auction.auction.domain.entity.Bid;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface BidMapper extends BaseMapper<Bid> {

    @Select("SELECT * FROM bid WHERE auction_id = #{auctionId} " +
            "ORDER BY amount DESC, bid_time ASC LIMIT 1")
    Bid findHighestBid(@Param("auctionId") Long auctionId);
}
