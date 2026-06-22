package com.auction.eventstore.repository;

import com.auction.eventstore.domain.EventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface EventStoreRepository extends MongoRepository<EventDocument, String> {

    Optional<EventDocument> findByEventId(String eventId);

    List<EventDocument> findByAggregateIdOrderBySequenceNumberAsc(String aggregateId);

    @Query(value = "{ 'aggregateId': ?0, 'sequenceNumber': { $gt: ?1 } }", sort = "{ 'sequenceNumber': 1 }")
    List<EventDocument> findByAggregateIdAndSequenceNumberGreaterThan(String aggregateId, long sequenceNumber);

    long countByAggregateId(String aggregateId);

    Optional<EventDocument> findTopByAggregateIdOrderBySequenceNumberDesc(String aggregateId);
}
