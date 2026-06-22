package com.auction.order.config;

import com.auction.common.feign.SeataFeignInterceptor;
import com.auction.common.feign.UserContextFeignInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignConfig {

    @Bean
    public SeataFeignInterceptor seataFeignInterceptor() {
        return new SeataFeignInterceptor();
    }

    @Bean
    public UserContextFeignInterceptor userContextFeignInterceptor() {
        return new UserContextFeignInterceptor();
    }
}