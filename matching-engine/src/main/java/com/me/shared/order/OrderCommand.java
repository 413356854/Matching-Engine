package com.me.shared.order;

import java.util.Locale;

/**
 * 委托命令，来自 Kafka topic {@code me.order.commands-batch} 的跨模块契约。
 *
 * <p>由币币模块的下单接口或外部生产者写入；委托模块只通过
 * {@code com.me.order.mq.OrderCommandListener} 消费，Kafka 是唯一的下单入口。
 *
 * <p>{@code requestId} 是幂等键，重复投递只会保存一笔委托；{@code orderId} 是委托的唯一标识，
 * 同一 orderId 不能重复保存。价格按 {@code 10^-priceScale}、数量按 {@code 10^-quantityScale}
 * 存储，{@code quoteScale} 是计价币账本精度；限价单价格必须大于 0，市价单价格固定为 0，
 * {@code symbol} 统一转为大写。
 *
 * <p>{@code amount} 只用于市价买入：它是前端按「参考价 × 1.05 × 数量」算出的计价币预算
 * （按 {@code 10^-quoteScale} 的最小单位），委托模块据此冻结计价币，撮合时按实际卖价
 * 把预算换成可买数量，未用完的部分退回。限价单、市价卖单固定为 0。
 */
public record OrderCommand(
        String requestId,
        long orderId,
        long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        long price,
        long quantity,
        int priceScale,
        int quantityScale,
        int quoteScale,
        long amount) {

    public OrderCommand {
        if (symbol != null) {
            symbol = symbol.strip().toUpperCase(Locale.ROOT);
        }
    }

    /**
     * 兼容旧命令 / 测试：没有精度信息时金额按 {@code price × quantity} 的原始乘积处理，
     * 且不携带市价买入预算。
     * 币币下单接口 / Kafka 入口会从交易对和币种配置补齐精度。
     */
    public OrderCommand(String requestId, long orderId, long userId, String symbol,
                        OrderSide side, OrderType type, long price, long quantity) {
        this(requestId, orderId, userId, symbol, side, type, price, quantity, 0, 0, 0, 0L);
    }

    /** 兼容不携带市价买入预算的调用方。 */
    public OrderCommand(String requestId, long orderId, long userId, String symbol,
                        OrderSide side, OrderType type, long price, long quantity,
                        int priceScale, int quantityScale, int quoteScale) {
        this(requestId, orderId, userId, symbol, side, type, price, quantity,
                priceScale, quantityScale, quoteScale, 0L);
    }

    public OrderCommand withScales(int priceScale, int quantityScale, int quoteScale) {
        return new OrderCommand(requestId, orderId, userId, symbol, side, type, price, quantity,
                priceScale, quantityScale, quoteScale, amount);
    }
}
