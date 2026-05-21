package com.auction.auction.service.impl;

import com.auction.auction.controller.dto.BidHistoryItem;
import com.auction.auction.controller.dto.PageResponse;
import com.auction.auction.domain.entity.Bid;
import com.auction.auction.repository.BidMapper;
import com.auction.auction.service.BidHistoryService;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BidHistoryServiceImpl implements BidHistoryService {

    private final BidMapper bidMapper;

    public BidHistoryServiceImpl(BidMapper bidMapper) {
        this.bidMapper = bidMapper;
    }

    @Override
    public PageResponse<BidHistoryItem> listBids(Long auctionId, long page, long size) {
        Page<Bid> pageRequest = new Page<>(page + 1, size);
        LambdaQueryWrapper<Bid> qw = new LambdaQueryWrapper<Bid>()
                .eq(Bid::getAuctionId, auctionId)
                .orderByDesc(Bid::getCreatedAt);
        Page<Bid> result = bidMapper.selectPage(pageRequest, qw);

        List<BidHistoryItem> items = result.getRecords().stream()
                .map(b -> BidHistoryItem.builder()
                        .bidderId(b.getUserId())
                        .bidderUsername(maskUsername(b.getUsername()))
                        .amount(b.getAmount())
                        .bidTime(b.getBidTime())
                        .build())
                .toList();

        return PageResponse.<BidHistoryItem>builder()
                .items(items)
                .total(result.getTotal())
                .page(page)
                .size(size)
                .build();
    }

    static String maskUsername(String username) {
        if (username == null || username.isEmpty()) {
            return "anonymous";
        }
        int len = username.length();
        if (len <= 2) {
            return "*".repeat(len);
        }
        if (len == 3) {
            return username.charAt(0) + "*" + username.charAt(2);
        }
        return username.charAt(0) + "***" + username.charAt(len - 1);
    }
}
