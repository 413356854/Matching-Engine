package com.me.shared.event;

import com.me.shared.order.OrderSide;
import java.time.Instant;

/**
 * 成交清算 DTO：撮合完成后由 {@code OrderMatchingService} 直接交给 {@code OrderService.onTrade}，
 * 不再写入 Kafka {@code me.trades}。
 *
 * <p>撮合以数量为基准：{@code amount = price × quantity} 是买卖双方交换的计价币金额
 * （交易对最小单位的整数）。委托清算按 {@code (takerOrderId, makerOrderId)} 幂等。
 */
public record TradeEvent(
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
