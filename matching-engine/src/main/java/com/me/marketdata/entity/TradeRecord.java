package com.me.marketdata.entity;

import com.me.shared.order.OrderSide;
import java.time.Instant;

/**
 * QuestDB {@code md_trade} 中的一条成交明细。
 *
 * <p>价格、数量、金额都是交易对最小单位的整数；{@code takerSide} 是吃单方向，
 * 历史数据缺失时可能为 {@code null}。
 */
public record TradeRecord(
        long tradeId,
        String symbol,
        long price,
        long quantity,
        long amount,
        OrderSide takerSide,
        Instant executedAt) {
}
