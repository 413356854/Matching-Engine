package com.me.matching.vo;

import java.time.Instant;
import java.util.List;

/**
 * 单个交易对的订单簿只读快照。
 *
 * <p>只包含限价单价格梯：买盘按价格降序、卖盘按价格升序（买一 / 卖一在列表首位）。
 * 市价单没有价格，不进入价格梯，只用 {@code marketBidCount} / {@code marketAskCount}
 * 报告挂单数量。所有价格、数量都是交易对最小单位的整数。
 *
 * <p>{@code bestBid} / {@code bestAsk} 在对应方向没有挂单时为 {@code null}。
 */
public record DepthSnapshot(
        String symbol,
        List<DepthLevel> bids,
        List<DepthLevel> asks,
        Long bestBid,
        Long bestAsk,
        int marketBidCount,
        int marketAskCount,
        Instant timestamp) {

    public DepthSnapshot {
        bids = bids == null ? List.of() : List.copyOf(bids);
        asks = asks == null ? List.of() : List.copyOf(asks);
    }

    /** 尚未有委托（或交易对没有订单簿）时的空快照。 */
    public static DepthSnapshot empty(String symbol) {
        return new DepthSnapshot(symbol, List.of(), List.of(), null, null, 0, 0, Instant.now());
    }
}
