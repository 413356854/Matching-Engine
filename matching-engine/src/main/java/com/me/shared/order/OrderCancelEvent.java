package com.me.shared.order;

import java.util.Locale;

/**
 * 撮合撤消单：委托模块把撤单请求聚合成一批后，进程内交给 {@code MatchingEngine} 撤单。
 *
 * <p>携带定位挂单所需的全部信息：{@code symbol} 定位订单簿，{@code side} / {@code type} /
 * {@code price} 定位所在的价格梯或市价簿，{@code orderId} 定位挂单。订单的方向、类型、价格
 * 在生命周期内不变，因此撤单可以直接命中目标结构，不需要扫描整个价格梯。
 *
 * <p>不在订单簿中的委托（已成交、从未挂单或重复撤销）由引擎返回“未找到”，委托模块据此
 * 跳过本次撤单，但仍会按委托的成交进度解冻剩余资金。
 */
public record OrderCancelEvent(
        long orderId,
        String symbol,
        OrderSide side,
        OrderType type,
        long price) {

    public OrderCancelEvent {
        if (symbol != null) {
            symbol = symbol.strip().toUpperCase(Locale.ROOT);
        }
    }
}
