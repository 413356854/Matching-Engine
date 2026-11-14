package com.me.shared.event;

import com.me.shared.order.OrderSide;
import java.time.Instant;

/**
 * 撮合对记录：委托模块在进程内撮合后，把每笔成交的 taker / maker 双方封装成
 * {@link MatchPairBatch} 写入 {@code me.match-pair-batch}。
 *
 * <p>行情模块以独立消费组消费该 topic 生成 K 线并保存成交明细；admin 模块以另一个
 * 独立消费组消费同一 topic，把每笔撮合对拆成买卖双方两条用户成交记录归档到 QuestDB
 * {@code user_trade}。
 *
 * <p>一条记录对应一笔成交（tick）。{@code (takerOrderId, makerOrderId)} 与委托清算的
 * 成交对幂等口径一致，作为成交的稳定身份；{@code tradeId} 在单个交易对内单调递增，
 * 用于查询排序。{@code takerUserId} / {@code makerUserId} 是双方用户，用于用户成交记录。
 *
 * <p>价格、数量、金额都是交易对最小单位的整数，{@code amount = price × quantity}。
 */
public record MatchPairEvent(
        long tradeId,
        String symbol,
        long takerOrderId,
        long makerOrderId,
        long takerUserId,
        long makerUserId,
        OrderSide takerSide,
        long price,
        long quantity,
        long amount,
        Instant executedAt) {
}
