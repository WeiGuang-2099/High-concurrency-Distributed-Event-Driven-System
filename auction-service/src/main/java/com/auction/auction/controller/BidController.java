package com.auction.auction.controller;

import com.auction.auction.controller.dto.BidHistoryItem;
import com.auction.auction.controller.dto.PageResponse;
import com.auction.auction.controller.dto.PlaceBidRequest;
import com.auction.auction.controller.dto.PlaceBidResponse;
import com.auction.common.exception.BusinessException;
import com.auction.common.security.UserContextHolder;
import com.auction.auction.service.BidHistoryService;
import com.auction.auction.service.BidService;
import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auctions/{auctionId}/bids")
public class BidController {

    private final BidService bidService;
    private final BidHistoryService bidHistoryService;

    public BidController(BidService bidService, BidHistoryService bidHistoryService) {
        this.bidService = bidService;
        this.bidHistoryService = bidHistoryService;
    }

    @PostMapping
    public ResponseEntity<ApiResponse<PlaceBidResponse>> placeBid(
            @PathVariable("auctionId") Long auctionId,
            @RequestBody @Valid PlaceBidRequest request) {
        UserContext ctx = UserContextHolder.get();
        if (ctx == null) {
            throw new BusinessException(401, "Authentication required");
        }
        PlaceBidResponse response = bidService.placeBid(
                auctionId, ctx.getUserId(), ctx.getUsername(), request.getAmount());
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.success(response));
    }

    @GetMapping
    public ResponseEntity<ApiResponse<PageResponse<BidHistoryItem>>> listBids(
            @PathVariable("auctionId") Long auctionId,
            @RequestParam(defaultValue = "0") long page,
            @RequestParam(defaultValue = "20") long size) {
        if (size > 100) {
            size = 100;
        }
        return ResponseEntity.ok(ApiResponse.success(
                bidHistoryService.listBids(auctionId, page, size)));
    }
}
