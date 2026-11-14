package com.me.currency.vo;

import java.util.List;

/**
 * 用户历史成交查询结果。
 *
 * <p>成功时 {@code trades} 按成交时间倒序（最新记录在前）；失败时 {@code reason} 是错误码，
 * {@code trades} 为空列表。{@code userId} / {@code symbol} 回显本次实际生效的筛选条件。
 */
public record UserTradeQueryView(
        boolean success,
        String reason,
        Long userId,
        String symbol,
        List<UserTradeView> trades) {

    public UserTradeQueryView {
        trades = trades == null ? List.of() : List.copyOf(trades);
    }

    public static UserTradeQueryView success(Long userId, String symbol,
                                             List<UserTradeView> trades) {
        return new UserTradeQueryView(true, null, userId, symbol, trades);
    }

    public static UserTradeQueryView rejected(Long userId, String symbol, String reason) {
        return new UserTradeQueryView(false, reason, userId, symbol, List.of());
    }
}
