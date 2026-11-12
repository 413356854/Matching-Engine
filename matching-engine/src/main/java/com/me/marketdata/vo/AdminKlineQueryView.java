package com.me.marketdata.vo;

import java.util.List;

/**
 * 管理端 K 线查询结果。
 *
 * <p>成功时 {@code klines} 按 bucket 起点倒序（最新在前）；失败时 {@code reason} 是错误码，
 * {@code klines} 为空列表。{@code symbol} / {@code interval} 回显实际生效的筛选条件，
 * 未筛选时为 {@code null}。
 */
public record AdminKlineQueryView(
        boolean success,
        String reason,
        String symbol,
        String interval,
        List<KlineView> klines) {

    public AdminKlineQueryView {
        klines = klines == null ? List.of() : List.copyOf(klines);
    }

    public static AdminKlineQueryView success(String symbol, String interval,
                                              List<KlineView> klines) {
        return new AdminKlineQueryView(true, null, symbol, interval, klines);
    }

    public static AdminKlineQueryView rejected(String symbol, String interval, String reason) {
        return new AdminKlineQueryView(false, reason, symbol, interval, List.of());
    }
}
