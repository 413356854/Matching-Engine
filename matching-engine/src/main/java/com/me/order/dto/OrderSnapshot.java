package com.me.order.dto;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;

/**
 * 一笔委托的完整快照，用于备份快照中的委托内存状态。
 *
 * <p>时间字段用 ISO-8601 字符串保存，避免快照序列化依赖日期模块；恢复时用
 * {@link java.time.Instant#parse(CharSequence)} 还原。
 */
public record OrderSnapshot(
        String requestId,
        long orderId,
        long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        long price,
        long quantity,
        String frozenCurrency,
        long frozenAmount,
        long filledQuantity,
        long settledQuoteAmount,
        OrderStatus status,
        long seq,
        String createdAt,
        String finalAt,
        int priceScale,
        int quantityScale,
        int quoteScale,
        boolean cancelRequested) {
}
