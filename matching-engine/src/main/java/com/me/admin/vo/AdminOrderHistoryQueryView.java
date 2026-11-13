package com.me.admin.vo;

import java.util.List;

/**
 * 管理端委托历史查询结果。
 *
 * <p>成功时 {@code orders} 按最终时间倒序（最新在前）；失败时 {@code reason} 是错误码，
 * {@code orders} 为空列表。筛选条件原样回显，未筛选时为 {@code null}。
 */
public record AdminOrderHistoryQueryView(
        boolean success,
        String reason,
        Long userId,
        String symbol,
        String status,
        List<AdminOrderHistoryView> orders) {

    public AdminOrderHistoryQueryView {
        orders = orders == null ? List.of() : List.copyOf(orders);
    }

    public static AdminOrderHistoryQueryView success(Long userId, String symbol, String status,
                                                     List<AdminOrderHistoryView> orders) {
        return new AdminOrderHistoryQueryView(true, null, userId, symbol, status, orders);
    }

    public static AdminOrderHistoryQueryView rejected(Long userId, String symbol, String status,
                                                      String reason) {
        return new AdminOrderHistoryQueryView(false, reason, userId, symbol, status, List.of());
    }
}
