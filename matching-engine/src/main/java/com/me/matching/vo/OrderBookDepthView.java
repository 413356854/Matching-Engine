package com.me.matching.vo;

import java.time.Instant;
import java.util.List;

/**
 * 订单簿查询接口的出参。
 *
 * <p>成功时 {@code bids} / {@code asks} 最多包含请求的 {@code depth} 个价位，
 * 买盘价格降序、卖盘价格升序；失败时 {@code reason} 是错误码，列表为空。
 */
public record OrderBookDepthView(
        boolean success,
        String reason,
        String symbol,
        int depth,
        List<DepthLevel> bids,
        List<DepthLevel> asks,
        Long bestBid,
        Long bestAsk,
        int marketBidCount,
        int marketAskCount,
        Instant timestamp) {

    public OrderBookDepthView {
        bids = bids == null ? List.of() : List.copyOf(bids);
        asks = asks == null ? List.of() : List.copyOf(asks);
    }

    public static OrderBookDepthView success(DepthSnapshot snapshot, int depth) {
        return new OrderBookDepthView(
                true,
                null,
                snapshot.symbol(),
                depth,
                snapshot.bids(),
                snapshot.asks(),
                snapshot.bestBid(),
                snapshot.bestAsk(),
                snapshot.marketBidCount(),
                snapshot.marketAskCount(),
                snapshot.timestamp());
    }

    public static OrderBookDepthView rejected(String symbol, String reason) {
        return new OrderBookDepthView(
                false, reason, symbol, 0,
                List.of(), List.of(), null, null, 0, 0, Instant.now());
    }
}
