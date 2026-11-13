package com.me.order.vo;

import com.me.shared.order.OrderHistoryRecord;
import java.time.Instant;

/**
 * 下单前端的委托历史视图：一条终态委托快照。
 *
 * <p>{@code status} 只可能是 {@code CANCELED}（撤单）或 {@code FILLED}（撮合完成）；
 * 价格、数量与金额均为对应精度的最小单位整数，前端按 {@code priceScale} /
 * {@code quantityScale} 换算展示。
 */
public record OrderHistoryView(
        long orderId,
        String requestId,
        long userId,
        String symbol,
        String side,
        String type,
        String status,
        long price,
        long quantity,
        long filledQuantity,
        String frozenCurrency,
        long frozenAmount,
        int priceScale,
        int quantityScale,
        int quoteScale,
        long seq,
        Instant createdAt,
        Instant finalAt) {

    public static OrderHistoryView from(OrderHistoryRecord record) {
        return new OrderHistoryView(
                record.orderId(),
                record.requestId(),
                record.userId(),
                record.symbol(),
                record.side().name(),
                record.type().name(),
                record.status().name(),
                record.price(),
                record.quantity(),
                record.filledQuantity(),
                record.frozenCurrency(),
                record.frozenAmount(),
                record.priceScale(),
                record.quantityScale(),
                record.quoteScale(),
                record.seq(),
                record.createdAt(),
                record.finalAt());
    }
}
