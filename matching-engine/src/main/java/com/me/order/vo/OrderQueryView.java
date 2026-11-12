package com.me.order.vo;

import java.util.List;

/**
 * 用户当前委托查询结果。
 *
 * <p>成功时 {@code orders} 是用户尚未完成的委托（{@code ACCEPTED} / {@code PAID} /
 * {@code PARTIALLY_FILLED}），按创建时间倒序（最新在前）；失败时 {@code reason} 是错误码，
 * {@code orders} 为空列表。{@code symbol} 回显实际生效的交易对筛选，未筛选时为 {@code null}。
 */
public record OrderQueryView(
        boolean success,
        String reason,
        long userId,
        String symbol,
        List<OrderView> orders) {

    public OrderQueryView {
        orders = orders == null ? List.of() : List.copyOf(orders);
    }

    public static OrderQueryView success(long userId, String symbol, List<OrderView> orders) {
        return new OrderQueryView(true, null, userId, symbol, orders);
    }

    public static OrderQueryView rejected(long userId, String symbol, String reason) {
        return new OrderQueryView(false, reason, userId, symbol, List.of());
    }
}
