package com.me.marketdata.vo;

import java.util.List;

/**
 * REST K 线查询结果。
 *
 * <p>成功时 {@code klines} 按 bucket 起点升序；失败时 {@code reason} 是错误码，
 * {@code klines} 为空列表。
 */
public record KlineQueryView(
        boolean success,
        String reason,
        String symbol,
        String interval,
        List<KlineView> klines) {

    public KlineQueryView {
        klines = klines == null ? List.of() : List.copyOf(klines);
    }

    public static KlineQueryView success(String symbol, String interval, List<KlineView> klines) {
        return new KlineQueryView(true, null, symbol, interval, klines);
    }

    public static KlineQueryView rejected(String symbol, String interval, String reason) {
        return new KlineQueryView(false, reason, symbol, interval, List.of());
    }
}
