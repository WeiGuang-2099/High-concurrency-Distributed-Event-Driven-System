package com.auction.ticket.config;

import io.seata.rm.datasource.DataSourceProxy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class SeataConfig {

    @Bean
    @ConditionalOnMissingBean(DataSourceProxy.class)
    public DataSourceProxy dataSourceProxy(DataSource dataSource) {
        return new DataSourceProxy(dataSource);
    }
}
