package com.me.currency.vo;

import com.me.shared.trade.UserTradeRecord;
import java.time.Instant;

/**
 * 用户历史成交视图：交易页面展示的一条成交快照。
 *
 * <p>{@code role} 为 {@code TAKER}（主动成交）/ {@code MAKER}（被动成交）；
 * {@code side} 是当前用户这一侧的方向。价格、数量、金额都是交易对最小单位的整数。
 */
public record UserTradeView(
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

    public static UserTradeView from(UserTradeRecord record) {
        return new UserTradeView(
                record.tradeId(),
                record.symbol(),
                record.userId(),
                record.orderId(),
                record.role().name(),
                record.counterOrderId(),
                record.side() == null ? null : record.side().name(),
                record.price(),
                record.quantity(),
                record.amount(),
                record.executedAt());
    }
}
