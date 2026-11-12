package com.me.marketdata.vo;

import com.me.marketdata.entity.TradeRecord;
import com.me.shared.order.OrderSide;
import java.time.Instant;

/**
 * 最新成交视图：对外暴露的一笔成交快照。
 *
 * <p>价格、数量、金额都是交易对最小单位的整数；{@code takerSide} 是吃单方向。
 */
public record TradeView(
        long tradeId,
        String symbol,
        long price,
        long quantity,
        long amount,
        OrderSide takerSide,
        Instant executedAt) {

    public static TradeView from(TradeRecord record) {
        return new TradeView(
                record.tradeId(),
                record.symbol(),
                record.price(),
                record.quantity(),
                record.amount(),
                record.takerSide(),
                record.executedAt());
    }
}
