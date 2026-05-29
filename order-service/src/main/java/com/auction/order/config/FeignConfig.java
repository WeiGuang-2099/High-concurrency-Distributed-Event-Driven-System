package com.auction.order.config;

import com.auction.common.feign.SeataFeignInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignConfig {

    @Bean
    public SeataFeignInterceptor seataFeignInterceptor() {
        return new SeataFeignInterceptor();
    }
}
