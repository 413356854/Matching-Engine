package com.me.shared.trade;

import com.me.shared.order.OrderSide;
import java.time.Instant;

/**
 * 用户成交记录查询结果（QuestDB {@code user_trade} 查询投影的只读快照）。
 *
 * <p>由 admin 模块的 QuestDB 访问层查询后映射；币币交易前端通过
 * {@link UserTradeQueryPort} 读取该契约，不直接访问 {@code user_trade} 表。
 *
 * <p>每个撮合对拆成 taker / maker 两条记录；{@code orderId} 是当前用户自己的委托，
 * {@code counterOrderId} 是成交对手的委托，{@code side} 是当前用户这一侧的方向。
 * 价格、数量、金额都是交易对最小单位的整数，按 {@code executedAt} 倒序（最新在前）。
 */
public record UserTradeRecord(
        long tradeId,
        String symbol,
        long userId,
        long orderId,
        UserTradeRole role,
        long counterOrderId,
        OrderSide side,
        long price,
        long quantity,
        long amount,
        Instant executedAt) {
}
