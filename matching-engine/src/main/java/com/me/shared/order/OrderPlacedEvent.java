package com.me.shared.order;

import java.time.Instant;

/**
 * 已支付委托事件：委托模块在冻结成功后，<b>进程内</b>交给 {@code MatchingEngine} 撮合，
 * 不再写入 Kafka {@code me.orders}。
 *
 * <p>价格按 {@code 10^-priceScale}、数量按 {@code 10^-quantityScale} 存储，成交金额按
 * {@code 10^-quoteScale} 的计价币最小单位计算（见 {@code com.me.shared.money.Amounts}）；
 * {@code frozenAmount} 也是计价币最小单位。
 *
 * <p><b>可能重复出现</b>：冻结回执可能因 Kafka 事务回滚重投或重复命令再次投递，同一
 * {@code orderId} 可能被多次送入撮合；撮合引擎按 orderId 去重，重复事件返回
 * {@code DUPLICATE_IGNORED}，不再产生行情/结算（演示版不重放输出）。
 */
public record OrderPlacedEvent(
        long orderId,
        long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        long price,
        long quantity,
        String frozenCurrency,
        long frozenAmount,
        Instant placedAt,
        int priceScale,
        int quantityScale,
        int quoteScale) {

    /** 兼容旧事件 / 测试：没有精度信息时金额按 {@code price × quantity} 的原始乘积处理。 */
    public OrderPlacedEvent(long orderId, long userId, String symbol, OrderSide side,
                            OrderType type, long price, long quantity, String frozenCurrency,
                            long frozenAmount, Instant placedAt) {
        this(orderId, userId, symbol, side, type, price, quantity, frozenCurrency, frozenAmount,
                placedAt, 0, 0, 0);
    }
}
