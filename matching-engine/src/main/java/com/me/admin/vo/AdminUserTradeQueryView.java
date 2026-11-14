package com.me.admin.vo;

import java.util.List;

/**
 * 管理端用户成交记录查询结果。
 *
 * <p>成功时 {@code trades} 按成交时间倒序（最新在前）；失败时 {@code reason} 是错误码，
 * {@code trades} 为空列表。筛选条件原样回显，未筛选时为 {@code null}。
 */
public record AdminUserTradeQueryView(
        boolean success,
        String reason,
        Long userId,
        String symbol,
        String role,
        List<AdminUserTradeView> trades) {

    public AdminUserTradeQueryView {
        trades = trades == null ? List.of() : List.copyOf(trades);
    }

    public static AdminUserTradeQueryView success(Long userId, String symbol, String role,
                                                  List<AdminUserTradeView> trades) {
        return new AdminUserTradeQueryView(true, null, userId, symbol, role, trades);
    }

    public static AdminUserTradeQueryView rejected(Long userId, String symbol, String role,
                                                   String reason) {
        return new AdminUserTradeQueryView(false, reason, userId, symbol, role, List.of());
    }
}
