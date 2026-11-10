package com.auction.ticket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.auction")
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "com.auction.ticket")
// OutboxRelay's @Scheduled poll is the only thing that moves ticket events from the outbox
// table to Kafka. Without this annotation the method is never called and the service still
// starts cleanly -- see OutboxRelayScheduleIT, which exists because that happened.
@EnableScheduling
public class TicketServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TicketServiceApplication.class, args);
    }
}
