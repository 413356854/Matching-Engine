package com.me.order.vo;

import java.util.List;

/**
 * 用户委托历史查询结果。
 *
 * <p>成功时 {@code orders} 是用户已进入终态（撤单 / 撮合完成）的委托，按最终时间倒序
 * （最新在前）；失败时 {@code reason} 是错误码，{@code orders} 为空列表。
 * {@code symbol} 回显实际生效的交易对筛选，未筛选时为 {@code null}。
 */
public record OrderHistoryQueryView(
        boolean success,
        String reason,
        long userId,
        String symbol,
        List<OrderHistoryView> orders) {

    public OrderHistoryQueryView {
        orders = orders == null ? List.of() : List.copyOf(orders);
    }

    public static OrderHistoryQueryView success(long userId, String symbol,
                                                List<OrderHistoryView> orders) {
        return new OrderHistoryQueryView(true, null, userId, symbol, orders);
    }

    public static OrderHistoryQueryView rejected(long userId, String symbol, String reason) {
        return new OrderHistoryQueryView(false, reason, userId, symbol, List.of());
    }
}
