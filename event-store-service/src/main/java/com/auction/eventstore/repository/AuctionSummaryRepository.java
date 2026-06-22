package com.auction.eventstore.repository;

import com.auction.eventstore.domain.AuctionSummaryView;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AuctionSummaryRepository extends MongoRepository<AuctionSummaryView, String> {
    Optional<AuctionSummaryView> findByAuctionId(Long auctionId);
}
