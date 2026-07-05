package com.auction.ticket.it;

import com.auction.common.exception.BusinessException;
import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.domain.enums.ReservationStatus;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.impl.TicketStockServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Ticket-side participant of the settlement saga against real MySQL:
 * settleReserve must be atomic under depletion, and settleRelease must be an
 * idempotent compensation (safe for the saga to retry).
 */
class SettleReservationIT extends AbstractTicketServiceIT {

    @Autowired
    private TicketStockServiceImpl ticketStockService;
    @Autowired
    private TicketStockMapper stockMapper;
    @Autowired
    private ReservationMapper reservationMapper;

    @Test
    void settleRelease_compensates_andIsIdempotent() {
        TicketStock stock = insertStock(920001L, 5);

        Long reservationId = ticketStockService.settleReserve(stock.getId(), 42L, 2);
        assertThat(reservationId).isNotNull();
        assertThat(stockMapper.selectById(stock.getId()).getReservedQuantity()).isEqualTo(2);

        ticketStockService.settleRelease(reservationId);
        assertThat(stockMapper.selectById(stock.getId()).getReservedQuantity()).isZero();
        Reservation reservation = reservationMapper.findById(reservationId);
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CANCELLED);

        // Saga retry: releasing again must not decrement the stock a second time.
        ticketStockService.settleRelease(reservationId);
        assertThat(stockMapper.selectById(stock.getId()).getReservedQuantity()).isZero();

        // Unknown reservation: treated as already released, not an error.
        ticketStockService.settleRelease(99999999L);
    }

    @Test
    void settleReserve_depletion_rejectsWithoutOversell() {
        TicketStock stock = insertStock(920002L, 1);

        Long first = ticketStockService.settleReserve(stock.getId(), 42L, 1);
        assertThat(first).isNotNull();

        assertThatThrownBy(() -> ticketStockService.settleReserve(stock.getId(), 43L, 1))
                .isInstanceOfSatisfying(BusinessException.class,
                        e -> assertThat(e.getCode()).isEqualTo(400));

        TicketStock after = stockMapper.selectById(stock.getId());
        assertThat(after.getReservedQuantity()).isEqualTo(1);
        assertThat(after.getTotalQuantity() - after.getReservedQuantity() - after.getSoldQuantity())
                .isZero();
    }

    private TicketStock insertStock(long eventId, int total) {
        TicketStock stock = new TicketStock();
        stock.setEventId(eventId);
        stock.setTicketType("AUCTION");
        stock.setTotalQuantity(total);
        stock.setReservedQuantity(0);
        stock.setSoldQuantity(0);
        stock.setVersion(0);
        stockMapper.insert(stock);
        return stock;
    }
}
