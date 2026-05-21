package com.auction.auction.service;

import com.auction.auction.controller.dto.AuctionResponse;
import com.auction.auction.controller.dto.CreateAuctionRequest;
import com.auction.auction.controller.dto.PageResponse;

import java.util.List;

public interface AuctionService {

    AuctionResponse createAuction(CreateAuctionRequest request, Long createdBy);

    PageResponse<AuctionResponse> listAuctions(long page, long size);

    AuctionResponse getAuction(Long auctionId);

    List<AuctionResponse> getHotAuctions();
}
