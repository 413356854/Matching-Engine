package com.me.order.vo;

import com.me.order.dto.OrderLookupRecord;
import com.me.shared.order.OrderHistoryRecord;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import java.time.Instant;

/**
 * 按 requestId 查询委托的统一结果。
 *
 * <p>{@code source} 表示命中的位置：
 * <ul>
 *   <li>{@code CURRENT}：委托模块内存中的当前委托（任意状态，含 {@code REJECTED}）；</li>
 *   <li>{@code HISTORY}：QuestDB {@code ord_history} 中已归档的终态委托
 *       （{@code CANCELED} / {@code FILLED}）。</li>
 * </ul>
 *
 * <p>查询顺序是先当前、后历史；两处都没有时返回 {@code success=false}、
 * {@code reason=ORDER_NOT_FOUND}。当前委托的 {@code finalAt} 只在终态时非空。
 */
public record OrderLookupView(
        boolean success,
        String reason,
        String source,
        Long orderId,
        String requestId,
        Long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        OrderStatus status,
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

    /** 当前内存委托。 */
    public static OrderLookupView current(OrderLookupRecord record) {
        return new OrderLookupView(
                true, null, "CURRENT",
                record.orderId(), record.requestId(), record.userId(), record.symbol(),
                record.side(), record.type(), record.status(), record.price(), record.quantity(),
                record.filledQuantity(), record.frozenCurrency(), record.frozenAmount(),
                record.priceScale(), record.quantityScale(), record.quoteScale(), record.seq(),
                record.createdAt(), record.finalAt());
    }

    /** 已归档的历史委托。 */
    public static OrderLookupView history(OrderHistoryRecord record) {
        return new OrderLookupView(
                true, null, "HISTORY",
                record.orderId(), record.requestId(), record.userId(), record.symbol(),
                record.side(), record.type(), record.status(), record.price(), record.quantity(),
                record.filledQuantity(), record.frozenCurrency(), record.frozenAmount(),
                record.priceScale(), record.quantityScale(), record.quoteScale(), record.seq(),
                record.createdAt(), record.finalAt());
    }

    /** 当前委托和历史都没有该 requestId。 */
    public static OrderLookupView notFound(long userId, String requestId) {
        return new OrderLookupView(
                false, "ORDER_NOT_FOUND", null,
                null, requestId, userId, null, null, null, null,
                0L, 0L, 0L, null, 0L, 0, 0, 0, 0L, null, null);
    }

    /** 参数或查询失败。 */
    public static OrderLookupView rejected(String reason) {
        return new OrderLookupView(
                false, reason, null,
                null, null, null, null, null, null, null,
                0L, 0L, 0L, null, 0L, 0, 0, 0, 0L, null, null);
    }
}
