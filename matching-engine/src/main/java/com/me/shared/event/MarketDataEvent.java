package com.me.shared.event;

import com.me.shared.order.OrderSide;
import java.time.Instant;

/**
 * 行情记录：委托模块在进程内撮合后，把每笔成交封装成 {@link MarketDataBatch} 写入
 * {@code me.marketdata-batch}，由行情模块以独立消费组消费，生成 K 线并保存成交明细。
 *
 * <p>一条记录对应一笔成交（tick），不含 userId。{@code (takerOrderId, makerOrderId)} 与委托清算的
 * 成交对幂等口径一致，作为成交的稳定身份；{@code tradeId} 在单个交易对内单调递增，用于查询排序。
 *
 * <p>价格、数量、金额都是交易对最小单位的整数，{@code amount = price × quantity}。
 */
public record MarketDataEvent(
        long tradeId,
        String symbol,
        long takerOrderId,
        long makerOrderId,
        OrderSide takerSide,
        long price,
        long quantity,
        long amount,
        Instant executedAt) {
}
