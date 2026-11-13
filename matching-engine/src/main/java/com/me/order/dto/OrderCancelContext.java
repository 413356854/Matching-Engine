package com.me.order.dto;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;

/**
 * 撤单准备阶段的委托快照。
 *
 * <p>由 {@code OrderService.prepareCancel} 在撤单线程池中并发查询得到，随后作为撮合撤消单
 * 的输入；撮合引擎按 {@code side} / {@code type} / {@code price} 直接定位并移除挂单，
 * 再由 {@code OrderService.applyCancel} 在第二阶段线程池中原子推进为 {@code CANCELED}。
 * 快照携带计算解冻金额所需的全部字段：
 *
 * <ul>
 *   <li>买单：冻结计价币 {@code frozenAmount}，已成交消耗 {@code settledQuoteAmount}；</li>
 *   <li>卖单：冻结基础币 {@code quantity}，已成交消耗 {@code filledQuantity}。</li>
 * </ul>
 */
public record OrderCancelContext(
        long orderId,
        long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        long price,
        OrderStatus status,
        long quantity,
        long filledQuantity,
        String frozenCurrency,
        long frozenAmount,
        long settledQuoteAmount) {
}
