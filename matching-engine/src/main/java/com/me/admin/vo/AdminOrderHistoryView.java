package com.me.admin.vo;

import com.me.admin.entity.OrderHistoryRow;
import java.time.Instant;

/**
 * 管理端委托历史视图：一条终态委托快照。
 *
 * <p>{@code status} 只可能是 {@code CANCELED} / {@code FILLED}；价格、数量与金额均为
 * 对应精度的最小单位整数，{@code finalAt} 是委托进入终态的时间。
 */
public record AdminOrderHistoryView(
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

    public static AdminOrderHistoryView from(OrderHistoryRow row) {
        return new AdminOrderHistoryView(
                row.orderId(),
                row.requestId(),
                row.userId(),
                row.symbol(),
                row.side().name(),
                row.type().name(),
                row.status().name(),
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
