package com.auction.eventstore.repository;

import com.auction.eventstore.domain.OrderTimelineEntry;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderTimelineRepository extends MongoRepository<OrderTimelineEntry, String> {
    List<OrderTimelineEntry> findByOrderIdOrderByTimestampAsc(Long orderId);
}
