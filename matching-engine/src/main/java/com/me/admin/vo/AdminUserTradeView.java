package com.me.admin.vo;

import com.me.admin.entity.UserTradeRow;
import java.time.Instant;

/**
 * 管理端用户成交记录视图：撮合对拆分后的一条用户成交。
 *
 * <p>{@code role} 为 {@code TAKER} / {@code MAKER}；{@code side} 是当前用户这一侧的
 * 方向（历史数据缺失时可能为 {@code null}）。价格、数量、金额都是最小单位整数。
 */
public record AdminUserTradeView(
        long tradeId,
        String symbol,
        long userId,
        long orderId,
        String role,
        long counterOrderId,
        String side,
        long price,
        long quantity,
        long amount,
        Instant executedAt) {

    public static AdminUserTradeView from(UserTradeRow row) {
        return new AdminUserTradeView(
                row.tradeId(),
                row.symbol(),
                row.userId(),
                row.orderId(),
                row.role().name(),
                row.counterOrderId(),
                row.side() == null ? null : row.side().name(),
                row.price(),
                row.quantity(),
                row.amount(),
                row.executedAt());
    }
}
