package com.auction.ticket.startup;

import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import com.auction.ticket.service.impl.TicketStockServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RedisStockWarmer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RedisStockWarmer.class);

    private final TicketStockMapper stockMapper;
    private final StringRedisTemplate redis;
    private final TicketStockServiceImpl ticketStockService;

    public RedisStockWarmer(TicketStockMapper stockMapper,
                            StringRedisTemplate redis,
                            TicketStockServiceImpl ticketStockService) {
        this.stockMapper = stockMapper;
        this.redis = redis;
        this.ticketStockService = ticketStockService;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Warming up Redis stock cache from MySQL...");
        try {
            List<TicketStock> stocks = stockMapper.selectList(null);
            for (TicketStock stock : stocks) {
                int available = stock.getTotalQuantity() - stock.getReservedQuantity() - stock.getSoldQuantity();
                String key = RedisKeys.stock(stock.getEventId(), stock.getTicketType());
                redis.opsForValue().set(key, String.valueOf(available));
            }
            ticketStockService.markReady();
            log.info("Redis stock cache warmed up: {} stock entries loaded", stocks.size());
        } catch (Exception e) {
            log.error("Failed to warm up Redis stock cache. Service will remain unavailable (503) until restarted.", e);
        }
    }
}
