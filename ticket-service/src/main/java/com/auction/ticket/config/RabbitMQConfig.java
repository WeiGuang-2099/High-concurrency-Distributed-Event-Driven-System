package com.auction.ticket.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    public static final String DELAY_EXCHANGE = "delay.exchange";
    public static final String STOCK_RELEASE_QUEUE = "stock-release-queue";
    public static final String STOCK_RELEASE_ROUTING_KEY = "stock.release";
    public static final String DLX = "stock-release.dlx";
    public static final String STOCK_RELEASE_DLQ = "stock-release-dlq";

    @Bean
    public CustomExchange delayExchange() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        return new CustomExchange(DELAY_EXCHANGE, "x-delayed-message", true, false, args);
    }

    /**
     * A release message that cannot be processed goes to the DLQ rather than back on the
     * queue. This queue gives back held stock, so a message stuck redelivering forever
     * would stall every timeout behind it and the stock would never be resold -- silently.
     * Same rule as the Kafka side: dead-letter, never skip and never spin (docs/adr/0006).
     *
     * <p><b>Deploying this to a broker that already has the queue requires deleting it
     * first.</b> Adding x-dead-letter-exchange changes the declaration, and RabbitMQ fails
     * a mismatched re-declare with PRECONDITION_FAILED rather than altering the queue.</p>
     */
    @Bean
    public Queue stockReleaseQueue() {
        return QueueBuilder.durable(STOCK_RELEASE_QUEUE)
                .withArgument("x-dead-letter-exchange", DLX)
                .withArgument("x-dead-letter-routing-key", STOCK_RELEASE_ROUTING_KEY)
                .build();
    }

    @Bean
    public DirectExchange stockReleaseDlx() {
        return new DirectExchange(DLX, true, false);
    }

    @Bean
    public Queue stockReleaseDlq() {
        return QueueBuilder.durable(STOCK_RELEASE_DLQ).build();
    }

    @Bean
    public Binding stockReleaseDlqBinding() {
        return BindingBuilder.bind(stockReleaseDlq())
                .to(stockReleaseDlx())
                .with(STOCK_RELEASE_ROUTING_KEY);
    }

    @Bean
    public Binding stockReleaseBinding() {
        return BindingBuilder.bind(stockReleaseQueue())
                .to(delayExchange())
                .with(STOCK_RELEASE_ROUTING_KEY)
                .noargs();
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
