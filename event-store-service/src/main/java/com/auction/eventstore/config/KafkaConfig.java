package com.auction.eventstore.config;

import com.auction.common.event.KafkaTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration mirroring the pattern used by notification-service:
 * StringDeserializer + JsonMessageConverter (with JavaTimeModule) handles all event
 * subtypes via Jackson polymorphic deserialization using the __TypeId__ header that
 * producers add automatically.
 */
@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // ---- Topic declarations (idempotent) ----
    @Bean
    public NewTopic auctionEventsTopic() {
        return TopicBuilder.name(KafkaTopics.AUCTION_EVENTS).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic ticketEventsTopic() {
        return TopicBuilder.name(KafkaTopics.TICKET_EVENTS).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_EVENTS).partitions(6).replicas(1).build();
    }

    // DLTs must exist with the same partition count as their source topics: the
    // dead-letter recoverer publishes to the SAME partition the record came from,
    // which fails if the broker auto-created the DLT with fewer partitions.
    @Bean
    public NewTopic auctionEventsDltTopic() {
        return TopicBuilder.name(KafkaTopics.AUCTION_EVENTS_DLT).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic ticketEventsDltTopic() {
        return TopicBuilder.name(KafkaTopics.TICKET_EVENTS_DLT).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_EVENTS_DLT).partitions(6).replicas(1).build();
    }

    // ---- Consumer factory (no group-id here; each @KafkaListener sets its own) ----
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> cf, KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        JsonMessageConverter converter = new JsonMessageConverter(mapper);
        // Resolve the concrete event class from the producer's __TypeId__ header.
        // Without TYPE_ID precedence the converter deserializes into the inferred
        // parameter type (BaseEvent), silently dropping every subtype field.
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
        typeMapper.addTrustedPackages(
                "com.auction.common.event",
                "com.auction.common.event.auction",
                "com.auction.common.event.ticket",
                "com.auction.common.event.order");
        converter.setTypeMapper(typeMapper);
        factory.setRecordMessageConverter(converter);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Retry 3 times with 1s gap, then publish to the source topic's DLT. Skipping
        // instead of dead-lettering would silently lose events from the source of truth.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(KafkaTopics.dltFor(record.topic()), record.partition()));
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L)));
        return factory;
    }
}

