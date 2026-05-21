package com.auction.auction.service;

import com.auction.auction.controller.dto.PlaceBidResponse;

import java.math.BigDecimal;

public interface BidService {

    PlaceBidResponse placeBid(Long auctionId, Long bidderId, String bidderUsername, BigDecimal amount);
}
