package com.me.marketdata.vo;

import java.util.List;

/**
 * 最新成交查询结果。
 *
 * <p>成功时 {@code trades} 按成交时间倒序（最新在前）；失败时 {@code reason} 是错误码，
 * {@code trades} 为空列表。
 */
public record TradeQueryView(
        boolean success,
        String reason,
        String symbol,
        List<TradeView> trades) {

    public TradeQueryView {
        trades = trades == null ? List.of() : List.copyOf(trades);
    }

    public static TradeQueryView success(String symbol, List<TradeView> trades) {
        return new TradeQueryView(true, null, symbol, trades);
    }

    public static TradeQueryView rejected(String symbol, String reason) {
        return new TradeQueryView(false, reason, symbol, List.of());
    }
}
