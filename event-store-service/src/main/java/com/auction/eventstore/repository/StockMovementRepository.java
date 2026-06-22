package com.auction.eventstore.repository;

import com.auction.eventstore.domain.StockMovementEntry;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StockMovementRepository extends MongoRepository<StockMovementEntry, String> {
    List<StockMovementEntry> findByEventIdOrderByTimestampAsc(Long eventId);
}
