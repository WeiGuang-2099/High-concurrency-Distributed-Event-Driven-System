package com.auction.auction.controller;

import com.auction.auction.controller.dto.AuctionResponse;
import com.auction.auction.controller.dto.PageResponse;
import com.auction.auction.service.AuctionService;
import com.auction.common.dto.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/auctions")
public class AuctionController {

    private final AuctionService auctionService;

    public AuctionController(AuctionService auctionService) {
        this.auctionService = auctionService;
    }

    @GetMapping
    public ResponseEntity<ApiResponse<PageResponse<AuctionResponse>>> list(
            @RequestParam(defaultValue = "0") long page,
            @RequestParam(defaultValue = "20") long size) {
        if (size > 100) {
            size = 100;
        }
        return ResponseEntity.ok(ApiResponse.success(auctionService.listAuctions(page, size)));
    }

    @GetMapping("/hot")
    public ResponseEntity<ApiResponse<List<AuctionResponse>>> hot() {
        return ResponseEntity.ok(ApiResponse.success(auctionService.getHotAuctions()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<AuctionResponse>> detail(@PathVariable("id") Long id) {
        return ResponseEntity.ok(ApiResponse.success(auctionService.getAuction(id)));
    }
}
