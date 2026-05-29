package com.auction.auction.controller;

import com.auction.auction.controller.dto.AuctionResponse;
import com.auction.auction.controller.dto.CreateAuctionRequest;
import com.auction.common.exception.BusinessException;
import com.auction.common.security.UserContextHolder;
import com.auction.auction.service.AuctionService;
import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/auctions")
public class AdminAuctionController {

    private final AuctionService auctionService;

    public AdminAuctionController(AuctionService auctionService) {
        this.auctionService = auctionService;
    }

    @PostMapping
    public ResponseEntity<ApiResponse<AuctionResponse>> createAuction(
            @RequestBody @Valid CreateAuctionRequest request) {
        UserContext ctx = UserContextHolder.get();
        if (ctx == null) {
            throw new BusinessException(401, "Authentication required");
        }
        if (!ctx.isAdmin()) {
            throw new BusinessException(403, "Admin role required");
        }
        AuctionResponse response = auctionService.createAuction(request, ctx.getUserId());
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.success(response));
    }
}
