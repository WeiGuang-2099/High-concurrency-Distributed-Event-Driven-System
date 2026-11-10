package com.auction.notification.config;

import com.auction.common.event.KafkaTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // DLTs for the topics this service consumes. Declared here (not only in
    // event-store-service) so notification-service can dead-letter on its own;
    // partition counts must match the source topics, see KafkaTopics.dltFor.
    @Bean
    public NewTopic notificationAuctionEventsDltTopic() {
        return TopicBuilder.name(KafkaTopics.AUCTION_EVENTS_DLT).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic notificationTicketEventsDltTopic() {
        return TopicBuilder.name(KafkaTopics.TICKET_EVENTS_DLT).partitions(6).replicas(1).build();
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-service");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory, KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Both listeners take an Acknowledgment and ack after handling. The container only
        // supplies one in a MANUAL ack mode; in the default BATCH mode spring-kafka refuses
        // to invoke the listener at all, and because that refusal is classified as
        // non-retryable the record goes straight to the DLT. The service then starts cleanly
        // and delivers nothing (NotificationPipelineIT).
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Use Jackson JSON converter with Java Time module for event deserialization
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
        // Retry 3 times with 1s gap, then publish to the source topic's DLT. Skipping
        // instead of dead-lettering would silently drop persisted user notifications
        // (OUTBID / AUCTION_SETTLED rows), not just the ephemeral WebSocket push.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(KafkaTopics.dltFor(record.topic()), record.partition()));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        // A record that is dead-lettered successfully is logged by nobody above DEBUG. That
        // silence is what let this service dead-letter every record it received and still
        // look healthy, so say it out loud, with the cause.
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                // Individual attempts are already logged by the listeners themselves.
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("Dead-lettered {}-{}@{} key={} to {}",
                        record.topic(), record.partition(), record.offset(), record.key(),
                        KafkaTopics.dltFor(record.topic()), ex);
            }
        });
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
