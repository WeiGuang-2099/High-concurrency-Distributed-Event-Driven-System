package com.auction.auction.service.impl;

import com.auction.auction.controller.dto.AuctionResponse;
import com.auction.auction.controller.dto.CreateAuctionRequest;
import com.auction.auction.controller.dto.PageResponse;
import com.auction.auction.domain.entity.Auction;
import com.auction.auction.domain.enums.AuctionStatus;
import com.auction.auction.exception.BusinessException;
import com.auction.auction.repository.AuctionMapper;
import com.auction.auction.service.AuctionService;
import com.auction.auction.service.RedisKeys;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.AuctionCreatedEvent;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class AuctionServiceImpl implements AuctionService {

    private static final Logger log = LoggerFactory.getLogger(AuctionServiceImpl.class);
    private static final String HOT_BIDCOUNT_ZSET = "auction:hot:bidcount";

    private final AuctionMapper auctionMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    @Value("${auction.hot-list.size:10}")
    private int hotListSize;

    @Value("${auction.hot-list.cache-ttl-seconds:30}")
    private long hotCacheTtlSeconds;

    public AuctionServiceImpl(AuctionMapper auctionMapper,
                              KafkaTemplate<String, Object> kafkaTemplate,
                              StringRedisTemplate redis,
                              ObjectMapper objectMapper) {
        this.auctionMapper = auctionMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    @Override
    public AuctionResponse createAuction(CreateAuctionRequest request, Long createdBy) {
        LocalDateTime now = LocalDateTime.now();
        if (request.getStartTime().isBefore(now)) {
            throw new BusinessException(400, "startTime must be in the future");
        }
        if (!request.getEndTime().isAfter(request.getStartTime())) {
            throw new BusinessException(400, "endTime must be after startTime");
        }

        Auction auction = new Auction();
        auction.setEventName(request.getEventName());
        auction.setDescription(request.getDescription());
        auction.setTicketTypeId(request.getTicketTypeId());
        auction.setStartingPrice(request.getStartingPrice());
        auction.setStatus(AuctionStatus.PENDING);
        auction.setStartTime(request.getStartTime());
        auction.setEndTime(request.getEndTime());
        auctionMapper.insert(auction);

        AuctionCreatedEvent event = new AuctionCreatedEvent(auction.getId());
        event.setEventName(auction.getEventName());
        event.setDescription(auction.getDescription());
        event.setTicketTypeId(auction.getTicketTypeId());
        event.setStartingPrice(auction.getStartingPrice());
        event.setStartTime(auction.getStartTime().toInstant(ZoneOffset.UTC));
        event.setEndTime(auction.getEndTime().toInstant(ZoneOffset.UTC));
        event.setCreatedBy(createdBy);
        publish(auction.getId(), event);

        return AuctionResponse.fromEntity(auction);
    }

    @Override
    public PageResponse<AuctionResponse> listAuctions(long page, long size) {
        Page<Auction> pageRequest = new Page<>(page + 1, size); // MyBatis-Plus is 1-based
        LambdaQueryWrapper<Auction> qw = new LambdaQueryWrapper<Auction>()
                .orderByDesc(Auction::getCreatedAt);
        Page<Auction> result = auctionMapper.selectPage(pageRequest, qw);

        List<AuctionResponse> items = result.getRecords().stream()
                .map(AuctionResponse::fromEntity)
                .toList();

        return PageResponse.<AuctionResponse>builder()
                .items(items)
                .total(result.getTotal())
                .page(page)
                .size(size)
                .build();
    }

    @Override
    public AuctionResponse getAuction(Long auctionId) {
        Auction auction = auctionMapper.selectById(auctionId);
        if (auction == null) {
            throw new BusinessException(404, "Auction not found");
        }
        return AuctionResponse.fromEntity(auction);
    }

    @Override
    public List<AuctionResponse> getHotAuctions() {
        String cacheKey = RedisKeys.hotAuctionsCache();
        String cached = redis.opsForValue().get(cacheKey);
        if (cached != null) {
            try {
                return objectMapper.readValue(cached,
                        objectMapper.getTypeFactory().constructCollectionType(List.class, AuctionResponse.class));
            } catch (JsonProcessingException e) {
                log.warn("Failed to read hot cache, refreshing", e);
            }
        }

        Set<ZSetOperations.TypedTuple<String>> top = redis.opsForZSet()
                .reverseRangeWithScores(HOT_BIDCOUNT_ZSET, 0, hotListSize - 1);

        List<AuctionResponse> result = new ArrayList<>();
        if (top != null) {
            for (ZSetOperations.TypedTuple<String> entry : top) {
                if (entry.getValue() == null) {
                    continue;
                }
                Long id = Long.parseLong(entry.getValue());
                Auction a = auctionMapper.selectById(id);
                if (a != null && a.getStatus() == AuctionStatus.ACTIVE) {
                    result.add(AuctionResponse.fromEntity(a));
                }
            }
        }

        // Fallback: if zset is empty (cold start), pick the most recent ACTIVE auctions
        if (result.isEmpty()) {
            Page<Auction> pageRequest = new Page<>(1, hotListSize);
            LambdaQueryWrapper<Auction> qw = new LambdaQueryWrapper<Auction>()
                    .eq(Auction::getStatus, AuctionStatus.ACTIVE)
                    .orderByDesc(Auction::getCreatedAt);
            Page<Auction> activePage = auctionMapper.selectPage(pageRequest, qw);
            result = activePage.getRecords().stream()
                    .map(AuctionResponse::fromEntity)
                    .toList();
        } else {
            // Sort by descending current_highest_bid as a tie-break / display order
            result.sort(Comparator.comparing(
                    AuctionResponse::getCurrentHighestBid,
                    Comparator.nullsLast(Comparator.reverseOrder())));
        }

        try {
            redis.opsForValue().set(cacheKey,
                    objectMapper.writeValueAsString(result),
                    hotCacheTtlSeconds, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            log.warn("Failed to cache hot list", e);
        }

        return result;
    }

    private void publish(Long auctionId, Object event) {
        kafkaTemplate.send(KafkaTopics.AUCTION_EVENTS, String.valueOf(auctionId), event);
    }
}
