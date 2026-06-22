package com.auction.eventstore.repository;

import com.auction.eventstore.domain.SnapshotDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface SnapshotRepository extends MongoRepository<SnapshotDocument, String> {

    Optional<SnapshotDocument> findTopByAggregateIdOrderBySequenceNumberDesc(String aggregateId);
}
