package com.me.shared.order;

import java.time.Instant;
import java.util.Locale;

/**
 * 委托历史事件：委托进入终态（{@link OrderStatus#CANCELED} / {@link OrderStatus#FILLED}）时的快照。
 *
 * <p>委托模块在撤单更新、成交清算、市价买入完成时收集该事件，按 symbol 聚合后发往
 * {@code me.order.history-batch}；admin 模块消费并归档到 QuestDB {@code ord_history}。
 *
 * <p>{@code finalAt} 是委托进入终态的时间，由第一次状态推进时确定，之后保持不变：
 * 同一委托的历史事件重投时携带相同时间戳，QuestDB 可按 {@code (final_at, order_id)} 去重。
 *
 * <p>价格按 {@code 10^-priceScale}、数量按 {@code 10^-quantityScale} 存储，
 * {@code filledQuantity} 是终态时的累计成交量（基础币最小单位）。撤单时
 * {@code filledQuantity} 可能小于 {@code quantity}（部分成交后撤单）。
 */
public record OrderHistoryEvent(
        long orderId,
        String requestId,
        long userId,
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

    public OrderHistoryEvent {
        if (symbol != null) {
            symbol = symbol.strip().toUpperCase(Locale.ROOT);
        }
    }

    /** 是否终态事件；只有撤单与撮合完成会进入历史。 */
    public boolean terminal() {
        return status == OrderStatus.CANCELED || status == OrderStatus.FILLED;
    }
}
