package com.auction.eventstore.repository;

import com.auction.eventstore.domain.BidHistoryEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BidHistoryRepository extends MongoRepository<BidHistoryEntry, String> {
    Page<BidHistoryEntry> findByAuctionIdOrderByBidTimeDesc(Long auctionId, Pageable pageable);
}
