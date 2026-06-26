package com.auction.auction.service;

import com.auction.auction.client.OrderFeignClient;
import com.auction.auction.client.TicketFeignClient;
import com.auction.common.dto.ApiResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("SettlementService - 拍卖结算补偿式 Saga")
class SettlementServiceTest {

    @Mock private TicketFeignClient ticketFeignClient;
    @Mock private OrderFeignClient orderFeignClient;

    @InjectMocks private SettlementService settlementService;

    private static final Long AUCTION_ID = 100L;
    private static final Long WINNER_ID = 7L;
    private static final Long TICKET_TYPE_ID = 55L;
    private static final BigDecimal AMOUNT = new BigDecimal("199.00");
    private static final Long RESERVATION_ID = 9001L;

    private ApiResponse<Map<String, Object>> reserveOk() {
        return ApiResponse.success(Map.of("reservationId", RESERVATION_ID));
    }

    @Test
    @DisplayName("两步都成功时不触发补偿,且不抛异常")
    void happyPath_noCompensation() {
        when(ticketFeignClient.settleReserve(any())).thenReturn(reserveOk());
        when(orderFeignClient.createAuctionOrder(any())).thenReturn(ApiResponse.success(Map.of("orderId", 1L)));

        settlementService.settle(AUCTION_ID, WINNER_ID, AMOUNT, TICKET_TYPE_ID);

        verify(ticketFeignClient).settleReserve(any());
        verify(orderFeignClient).createAuctionOrder(any());
        verify(ticketFeignClient, never()).settleRelease(any());
    }

    @Test
    @DisplayName("建单失败时释放已预留库存(补偿),然后抛出异常")
    void orderFails_compensatesReservation() {
        when(ticketFeignClient.settleReserve(any())).thenReturn(reserveOk());
        when(orderFeignClient.createAuctionOrder(any()))
                .thenReturn(ApiResponse.error(500, "order db down"));

        assertThatThrownBy(() ->
                settlementService.settle(AUCTION_ID, WINNER_ID, AMOUNT, TICKET_TYPE_ID))
                .isInstanceOf(RuntimeException.class);

        // The reserved stock for THIS reservation must be released.
        verify(ticketFeignClient).settleRelease(eq(Map.of("reservationId", RESERVATION_ID)));
    }

    @Test
    @DisplayName("建单抛异常(如 Feign 超时)时同样触发补偿")
    void orderThrows_compensatesReservation() {
        when(ticketFeignClient.settleReserve(any())).thenReturn(reserveOk());
        when(orderFeignClient.createAuctionOrder(any()))
                .thenThrow(new RuntimeException("feign timeout"));

        assertThatThrownBy(() ->
                settlementService.settle(AUCTION_ID, WINNER_ID, AMOUNT, TICKET_TYPE_ID))
                .isInstanceOf(RuntimeException.class);

        verify(ticketFeignClient).settleRelease(eq(Map.of("reservationId", RESERVATION_ID)));
    }

    @Test
    @DisplayName("库存预留失败时直接抛出,且不调用建单或补偿")
    void reserveFails_noOrderNoCompensation() {
        when(ticketFeignClient.settleReserve(any()))
                .thenReturn(ApiResponse.error(400, "Out of stock"));

        assertThatThrownBy(() ->
                settlementService.settle(AUCTION_ID, WINNER_ID, AMOUNT, TICKET_TYPE_ID))
                .isInstanceOf(RuntimeException.class);

        verify(orderFeignClient, never()).createAuctionOrder(any());
        verify(ticketFeignClient, never()).settleRelease(any());
    }

    @Test
    @DisplayName("补偿调用本身失败时不掩盖原始结算异常")
    void compensationFails_originalErrorStillPropagates() {
        when(ticketFeignClient.settleReserve(any())).thenReturn(reserveOk());
        when(orderFeignClient.createAuctionOrder(any()))
                .thenReturn(ApiResponse.error(500, "order db down"));
        when(ticketFeignClient.settleRelease(any()))
                .thenThrow(new RuntimeException("release feign down"));

        assertThatThrownBy(() ->
                settlementService.settle(AUCTION_ID, WINNER_ID, AMOUNT, TICKET_TYPE_ID))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Order creation failed");

        verify(ticketFeignClient).settleRelease(any());
    }
}
