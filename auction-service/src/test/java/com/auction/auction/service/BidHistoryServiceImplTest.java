package com.auction.auction.service;

import com.auction.auction.controller.dto.BidHistoryItem;
import com.auction.auction.controller.dto.PageResponse;
import com.auction.auction.domain.entity.Bid;
import com.auction.auction.repository.BidMapper;
import com.auction.auction.service.impl.BidHistoryServiceImpl;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("BidHistoryService - 竞价历史与用户名脱敏")
class BidHistoryServiceImplTest {

    @Mock private BidMapper bidMapper;

    @InjectMocks private BidHistoryServiceImpl bidHistoryService;

    // ==================== Username Masking ====================

    @Nested
    @DisplayName("maskUsername() 用户名脱敏 (via listBids)")
    class MaskUsernameTest {

        private Bid createBidWithUsername(String username) {
            Bid bid = new Bid();
            bid.setId(1L);
            bid.setAuctionId(100L);
            bid.setUserId(1L);
            bid.setUsername(username);
            bid.setAmount(new BigDecimal("100.00"));
            bid.setBidTime(LocalDateTime.now());
            return bid;
        }

        private PageResponse<BidHistoryItem> runWithBid(Bid bid) {
            Page<Bid> mockPage = new Page<>(1, 20);
            mockPage.setRecords(bid == null ? Collections.emptyList() : Collections.singletonList(bid));
            mockPage.setTotal(bid == null ? 0 : 1);
            when(bidMapper.selectPage(any(Page.class), any())).thenReturn(mockPage);
            return bidHistoryService.listBids(100L, 0, 20);
        }

        @Test
        @DisplayName("长用户名 - 首尾保留中间打码")
        void maskUsername_long() {
            PageResponse<BidHistoryItem> resp = runWithBid(createBidWithUsername("johndoe"));
            assertThat(resp.getItems().get(0).getBidderUsername()).isEqualTo("j***e");
        }

        @Test
        @DisplayName("3字符用户名 - 保留首尾中间打星")
        void maskUsername_3chars() {
            PageResponse<BidHistoryItem> resp = runWithBid(createBidWithUsername("abc"));
            assertThat(resp.getItems().get(0).getBidderUsername()).isEqualTo("a*c");
        }

        @Test
        @DisplayName("2字符用户名 - 全部打星")
        void maskUsername_2chars() {
            PageResponse<BidHistoryItem> resp = runWithBid(createBidWithUsername("ab"));
            assertThat(resp.getItems().get(0).getBidderUsername()).isEqualTo("**");
        }

        @Test
        @DisplayName("1字符用户名 - 全部打星")
        void maskUsername_1char() {
            PageResponse<BidHistoryItem> resp = runWithBid(createBidWithUsername("a"));
            assertThat(resp.getItems().get(0).getBidderUsername()).isEqualTo("*");
        }

        @Test
        @DisplayName("null用户名 - 返回anonymous")
        void maskUsername_null() {
            PageResponse<BidHistoryItem> resp = runWithBid(createBidWithUsername(null));
            assertThat(resp.getItems().get(0).getBidderUsername()).isEqualTo("anonymous");
        }

        @Test
        @DisplayName("空字符串用户名 - 返回anonymous")
        void maskUsername_empty() {
            PageResponse<BidHistoryItem> resp = runWithBid(createBidWithUsername(""));
            assertThat(resp.getItems().get(0).getBidderUsername()).isEqualTo("anonymous");
        }
    }

    // ==================== listBids ====================

    @Nested
    @DisplayName("listBids() 竞价历史列表")
    class ListBidsTest {

        @Test
        @DisplayName("正常查询 - 返回脱敏后的用户名")
        void listBids_success() {
            Bid bid1 = new Bid();
            bid1.setId(1L);
            bid1.setAuctionId(100L);
            bid1.setUserId(1L);
            bid1.setUsername("alice");
            bid1.setAmount(new BigDecimal("150.00"));
            bid1.setBidTime(LocalDateTime.now());

            Bid bid2 = new Bid();
            bid2.setId(2L);
            bid2.setAuctionId(100L);
            bid2.setUserId(2L);
            bid2.setUsername("bob");
            bid2.setAmount(new BigDecimal("200.00"));
            bid2.setBidTime(LocalDateTime.now());

            Page<Bid> mockPage = new Page<>(1, 20);
            mockPage.setRecords(Arrays.asList(bid1, bid2));
            mockPage.setTotal(2);

            when(bidMapper.selectPage(any(Page.class), any())).thenReturn(mockPage);

            PageResponse<BidHistoryItem> response = bidHistoryService.listBids(100L, 0, 20);

            assertThat(response.getItems()).hasSize(2);
            assertThat(response.getItems().get(0).getBidderUsername()).isEqualTo("a***e"); // alice (len=5) -> a***e
            assertThat(response.getItems().get(1).getBidderUsername()).isEqualTo("b*b"); // bob (len=3) -> b*b
            assertThat(response.getTotal()).isEqualTo(2);
        }

        @Test
        @DisplayName("无竞价记录 - 返回空列表")
        void listBids_empty() {
            Page<Bid> mockPage = new Page<>(1, 20);
            mockPage.setRecords(Collections.emptyList());
            mockPage.setTotal(0);

            when(bidMapper.selectPage(any(Page.class), any())).thenReturn(mockPage);

            PageResponse<BidHistoryItem> response = bidHistoryService.listBids(999L, 0, 20);

            assertThat(response.getItems()).isEmpty();
            assertThat(response.getTotal()).isEqualTo(0);
        }
    }
}
