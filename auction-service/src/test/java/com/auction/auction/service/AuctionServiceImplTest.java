package com.auction.auction.service;

import com.auction.auction.controller.dto.AuctionResponse;
import com.auction.auction.controller.dto.CreateAuctionRequest;
import com.auction.auction.controller.dto.PageResponse;
import com.auction.auction.domain.entity.Auction;
import com.auction.auction.domain.enums.AuctionStatus;
import com.auction.common.exception.BusinessException;
import com.auction.auction.repository.AuctionMapper;
import com.auction.auction.service.impl.AuctionServiceImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AuctionService - 拍卖管理")
class AuctionServiceImplTest {

    @Mock private AuctionMapper auctionMapper;
    @Mock private KafkaTemplate<String, Object> kafkaTemplate;
    @Mock private StringRedisTemplate redis;
    @Mock private ObjectMapper objectMapper;
    @Mock private ValueOperations<String, String> valueOperations;

    @InjectMocks private AuctionServiceImpl auctionService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(auctionService, "hotListSize", 10);
        ReflectionTestUtils.setField(auctionService, "hotCacheTtlSeconds", 30L);
        lenient().when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    // ==================== createAuction ====================

    @Nested
    @DisplayName("createAuction() 创建拍卖")
    class CreateAuctionTest {

        @Test
        @DisplayName("正常创建拍卖 - 设置状态为 PENDING")
        void createAuction_success() {
            CreateAuctionRequest request = new CreateAuctionRequest();
            request.setEventName("Concert Tickets");
            request.setDescription("VIP concert");
            request.setTicketTypeId(1L);
            request.setStartingPrice(new BigDecimal("100.00"));
            request.setStartTime(LocalDateTime.now().plusDays(1));
            request.setEndTime(LocalDateTime.now().plusDays(2));

            doAnswer(invocation -> {
                Auction auction = invocation.getArgument(0);
                auction.setId(1L);
                return 1;
            }).when(auctionMapper).insert(any(Auction.class));

            AuctionResponse response = auctionService.createAuction(request, 10L);

            assertThat(response).isNotNull();
            assertThat(response.getId()).isEqualTo(1L);
            assertThat(response.getEventName()).isEqualTo("Concert Tickets");
            assertThat(response.getStatus()).isEqualTo(AuctionStatus.PENDING);
            assertThat(response.getStartingPrice()).isEqualByComparingTo("100.00");

            // Verify event was published
            verify(kafkaTemplate).send(eq("auction-events"), eq("1"), any());
        }

        @Test
        @DisplayName("开始时间是过去 - 抛出400异常")
        void createAuction_startTimeInPast_throws400() {
            CreateAuctionRequest request = new CreateAuctionRequest();
            request.setEventName("Test");
            request.setStartingPrice(new BigDecimal("100.00"));
            request.setStartTime(LocalDateTime.now().minusDays(1));
            request.setEndTime(LocalDateTime.now().plusDays(1));

            assertThatThrownBy(() -> auctionService.createAuction(request, 10L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(400));

            verify(auctionMapper, never()).insert(any());
        }

        @Test
        @DisplayName("结束时间早于开始时间 - 抛出400异常")
        void createAuction_endBeforeStart_throws400() {
            CreateAuctionRequest request = new CreateAuctionRequest();
            request.setEventName("Test");
            request.setStartingPrice(new BigDecimal("100.00"));
            request.setStartTime(LocalDateTime.now().plusDays(2));
            request.setEndTime(LocalDateTime.now().plusDays(1));

            assertThatThrownBy(() -> auctionService.createAuction(request, 10L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(400));
        }

        @Test
        @DisplayName("新拍卖的初始最高出价应为null")
        void createAuction_currentHighestBidIsNull() {
            CreateAuctionRequest request = new CreateAuctionRequest();
            request.setEventName("Test");
            request.setStartingPrice(new BigDecimal("100.00"));
            request.setStartTime(LocalDateTime.now().plusDays(1));
            request.setEndTime(LocalDateTime.now().plusDays(2));

            doAnswer(invocation -> {
                Auction auction = invocation.getArgument(0);
                auction.setId(1L);
                return 1;
            }).when(auctionMapper).insert(any(Auction.class));

            ArgumentCaptor<Auction> captor = ArgumentCaptor.forClass(Auction.class);
            auctionService.createAuction(request, 10L);
            verify(auctionMapper).insert(captor.capture());

            Auction saved = captor.getValue();
            assertThat(saved.getCurrentHighestBid()).isNull();
            assertThat(saved.getCurrentHighestBidderId()).isNull();
            assertThat(saved.getWinnerId()).isNull();
        }
    }

    // ==================== getAuction ====================

    @Nested
    @DisplayName("getAuction() 获取拍卖详情")
    class GetAuctionTest {

        @Test
        @DisplayName("正常获取拍卖详情")
        void getAuction_success() {
            Auction auction = new Auction();
            auction.setId(1L);
            auction.setEventName("Test Auction");
            auction.setStatus(AuctionStatus.ACTIVE);
            auction.setStartingPrice(new BigDecimal("100.00"));

            when(auctionMapper.selectById(1L)).thenReturn(auction);

            AuctionResponse response = auctionService.getAuction(1L);

            assertThat(response).isNotNull();
            assertThat(response.getId()).isEqualTo(1L);
            assertThat(response.getEventName()).isEqualTo("Test Auction");
        }

        @Test
        @DisplayName("拍卖不存在 - 抛出404异常")
        void getAuction_notFound_throws404() {
            when(auctionMapper.selectById(999L)).thenReturn(null);

            assertThatThrownBy(() -> auctionService.getAuction(999L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }
    }

    // ==================== listAuctions ====================

    @Nested
    @DisplayName("listAuctions() 分页列表")
    class ListAuctionsTest {

        @Test
        @DisplayName("正常分页查询")
        void listAuctions_success() {
            Auction auction1 = new Auction();
            auction1.setId(1L);
            auction1.setEventName("Auction 1");
            auction1.setStatus(AuctionStatus.PENDING);

            Page<Auction> mockPage = new Page<>(1, 20);
            mockPage.setRecords(Collections.singletonList(auction1));
            mockPage.setTotal(1);

            when(auctionMapper.selectPage(any(Page.class), any())).thenReturn(mockPage);

            PageResponse<AuctionResponse> response = auctionService.listAuctions(0, 20);

            assertThat(response).isNotNull();
            assertThat(response.getTotal()).isEqualTo(1);
            assertThat(response.getPage()).isEqualTo(0);
            assertThat(response.getSize()).isEqualTo(20);
            assertThat(response.getItems()).hasSize(1);
            assertThat(response.getItems().get(0).getEventName()).isEqualTo("Auction 1");
        }

        @Test
        @DisplayName("空列表 - 返回空结果")
        void listAuctions_empty() {
            Page<Auction> mockPage = new Page<>(1, 20);
            mockPage.setRecords(Collections.emptyList());
            mockPage.setTotal(0);

            when(auctionMapper.selectPage(any(Page.class), any())).thenReturn(mockPage);

            PageResponse<AuctionResponse> response = auctionService.listAuctions(0, 20);

            assertThat(response.getItems()).isEmpty();
            assertThat(response.getTotal()).isEqualTo(0);
        }
    }
}
