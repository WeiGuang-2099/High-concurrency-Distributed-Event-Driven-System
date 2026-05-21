package com.auction.auction.service;

import com.auction.auction.controller.dto.BidHistoryItem;
import com.auction.auction.controller.dto.PageResponse;

public interface BidHistoryService {

    PageResponse<BidHistoryItem> listBids(Long auctionId, long page, long size);
}
