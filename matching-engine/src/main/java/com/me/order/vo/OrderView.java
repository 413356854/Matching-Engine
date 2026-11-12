package com.me.order.vo;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import java.time.Instant;

/**
 * 委托视图：已保存委托的完整快照。
 *
 * <p>{@code frozenCurrency} / {@code frozenAmount} 是下单时预占的资金：
 * 买单冻结计价币（{@code price × quantity}），卖单冻结基础币（{@code quantity}）。
 *
 * <p>{@code filledQuantity} 是累计成交量（基础币），成交清算时推进；
 * {@code quantity - filledQuantity} 即未成交数量。
 *
 * <p>{@code seq} 是同一委托内的状态变更序号，创建时为 1，冻结回执、成交清算推进状态时递增。
 */
public record OrderView(
        long orderId,
        long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        long price,
        long quantity,
        long filledQuantity,
        String frozenCurrency,
        long frozenAmount,
        OrderStatus status,
        long seq,
        Instant createdAt) {
}
