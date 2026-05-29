package com.auction.ticket.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;

@Configuration
public class RedisConfig {

    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DefaultRedisScript<List> reserveTicketScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/reserve_ticket.lua"));
        script.setResultType(List.class);
        return script;
    }

    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DefaultRedisScript<List> releaseTicketScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/release_ticket.lua"));
        script.setResultType(List.class);
        return script;
    }
}
