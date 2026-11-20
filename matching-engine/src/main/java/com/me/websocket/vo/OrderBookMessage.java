package com.me.websocket.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.me.marketdata.vo.KlineView;
import com.me.marketdata.vo.TradeView;
import com.me.matching.vo.DepthLevel;
import com.me.matching.vo.DepthSnapshot;
import java.time.Instant;
import java.util.List;

/**
 * 服务端推给前端的统一消息。
 *
 * <p>{@code type} 区分消息用途：
 * <ul>
 *   <li>{@code subscribed} / {@code unsubscribed}：订阅变更回执，只带 symbol / depth；</li>
 *   <li>{@code orderbook}：盘口快照，带买卖梯、买一/卖一、最新成交、
 *       各周期最新 K 线与快照时间；</li>
 *   <li>{@code error}：协议错误，{@code reason} 是错误码。</li>
 * </ul>
 * 与消息用途无关的字段为 {@code null} 时不会出现在 JSON 中。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderBookMessage(
        String type,
        String symbol,
        Integer depth,
        List<DepthLevel> bids,
        List<DepthLevel> asks,
        Long bestBid,
        Long bestAsk,
        List<TradeView> trades,
        List<KlineView> klines,
        Instant timestamp,
        String reason) {

    public static OrderBookMessage snapshot(DepthSnapshot snapshot, int depth,
                                            List<TradeView> trades, List<KlineView> klines) {
        return new OrderBookMessage(
                "orderbook",
                snapshot.symbol(),
                depth,
                snapshot.bids(),
                snapshot.asks(),
                snapshot.bestBid(),
                snapshot.bestAsk(),
                trades,
                klines,
                snapshot.timestamp(),
                null);
    }

    public static OrderBookMessage subscribed(String symbol, int depth) {
        return new OrderBookMessage(
                "subscribed", symbol, depth,
                null, null, null, null, null, null, Instant.now(), null);
    }

    public static OrderBookMessage unsubscribed() {
        return new OrderBookMessage(
                "unsubscribed", null, null,
                null, null, null, null, null, null, Instant.now(), null);
    }

    public static OrderBookMessage error(String symbol, String reason) {
        return new OrderBookMessage(
                "error", symbol, null,
                null, null, null, null, null, null, Instant.now(), reason);
    }
}
