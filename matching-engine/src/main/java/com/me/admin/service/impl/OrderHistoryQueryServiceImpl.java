package com.me.admin.service.impl;

import com.me.admin.entity.OrderHistoryRow;
import com.me.admin.repository.OrderHistoryRepository;
import com.me.admin.service.OrderHistoryQueryService;
import com.me.shared.order.OrderHistoryQueryPort;
import com.me.shared.order.OrderHistoryRecord;
import com.me.shared.order.OrderStatus;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 委托历史查询实现：直接委托 QuestDB 访问层。
 *
 * <p>同时实现 shared 的 {@link OrderHistoryQueryPort}，供下单前端查询当前用户的历史委托；
 * 管理端使用带状态筛选的 {@link #list(Long, String, OrderStatus, int)}。
 */
@Service
public class OrderHistoryQueryServiceImpl
        implements OrderHistoryQueryService, OrderHistoryQueryPort {

    private final OrderHistoryRepository repository;

    public OrderHistoryQueryServiceImpl(OrderHistoryRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<OrderHistoryRow> list(Long userId, String symbol, OrderStatus status, int limit) {
        return repository.findHistory(userId, symbol, status, limit);
    }

    @Override
    public List<OrderHistoryRecord> listByUser(long userId, String symbol, int limit) {
        return repository.findHistory(userId, symbol, null, limit).stream()
                .map(OrderHistoryQueryServiceImpl::toRecord)
                .toList();
    }

    private static OrderHistoryRecord toRecord(OrderHistoryRow row) {
        return new OrderHistoryRecord(
                row.orderId(),
                row.requestId(),
                row.userId(),
                row.symbol(),
                row.side(),
                row.type(),
                row.status(),
                row.price(),
                row.quantity(),
                row.filledQuantity(),
                row.frozenCurrency(),
                row.frozenAmount(),
                row.priceScale(),
                row.quantityScale(),
                row.quoteScale(),
                row.seq(),
                row.createdAt(),
                row.finalAt());
    }
}
