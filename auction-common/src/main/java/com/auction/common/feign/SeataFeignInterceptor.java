package com.auction.common.feign;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import io.seata.core.context.RootContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeataFeignInterceptor implements RequestInterceptor {

    private static final Logger log = LoggerFactory.getLogger(SeataFeignInterceptor.class);

    @Override
    public void apply(RequestTemplate template) {
        String xid = RootContext.getXID();
        if (xid != null && !xid.isEmpty()) {
            template.header("TX_XID", xid);
            log.debug("Propagated Seata XID={} to Feign request", xid);
        }
    }
}
