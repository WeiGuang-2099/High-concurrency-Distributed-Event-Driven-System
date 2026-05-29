package com.auction.ticket.repository;

import com.auction.ticket.domain.entity.TicketStock;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface TicketStockMapper extends BaseMapper<TicketStock> {

    @Select("SELECT * FROM ticket_stock WHERE event_id = #{eventId}")
    List<TicketStock> findByEventId(@Param("eventId") Long eventId);

    @Select("SELECT * FROM ticket_stock WHERE event_id = #{eventId} AND ticket_type = #{ticketType}")
    TicketStock findByEventIdAndTicketType(@Param("eventId") Long eventId, @Param("ticketType") String ticketType);

    @Update("UPDATE ticket_stock SET reserved_quantity = reserved_quantity + #{quantity}, version = version + 1 WHERE id = #{id}")
    int incrementReserved(@Param("id") Long id, @Param("quantity") int quantity);

    @Update("UPDATE ticket_stock SET reserved_quantity = reserved_quantity - #{quantity}, sold_quantity = sold_quantity + #{quantity}, version = version + 1 WHERE id = #{id} AND reserved_quantity >= #{quantity}")
    int decrementReservedAndIncrementSold(@Param("id") Long id, @Param("quantity") int quantity);

    @Update("UPDATE ticket_stock SET reserved_quantity = reserved_quantity - #{quantity}, version = version + 1 WHERE id = #{id} AND reserved_quantity >= #{quantity}")
    int decrementReserved(@Param("id") Long id, @Param("quantity") int quantity);
}
