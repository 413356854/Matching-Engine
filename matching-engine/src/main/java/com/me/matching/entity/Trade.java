package com.me.matching.entity;

import com.me.shared.money.Amounts;
import com.me.shared.order.OrderSide;

/**
 * 一笔成交。
 *
 * <p><b>撮合以数量为基准</b>：成交量 = min(新单剩余量, 挂单剩余量)；
 * <b>支付 / 交换金额</b> 按价格 / 数量 / 计价币精度换算成计价币最小单位
 * （见 {@link Amounts#quoteAmount}）。
 *
 * <p>成交价规则：限价对限价取挂单（maker）价格；限价对市价取限价单价格
 * （市价单价格恒为 0，不能作为成交价）。
 *
 * <p>{@code tradeId} 在单个交易对内单调递增；跨交易对唯一定位需要 {@code symbol + tradeId}。
 *
 * <p>纯 JDK，无框架依赖。
 */
public record Trade(
        long tradeId,
        String symbol,
        long takerOrderId,
        long makerOrderId,
        long takerUserId,
        long makerUserId,
        OrderSide takerSide,
        long price,
        long quantity,
        long amount) {

    public Trade {
        if (quantity <= 0) {
            throw new IllegalArgumentException("trade quantity must be positive");
        }
        if (price <= 0) {
            throw new IllegalArgumentException("trade price must be positive");
        }
        if (amount <= 0) {
            throw new IllegalArgumentException("trade amount must be positive");
        }
    }

    /**
     * 按交易对精度生成成交金额；金额溢出 / 无法整除时抛出异常，早于订单簿状态变更。
     */
    public static Trade matched(long tradeId, String symbol,
                                long takerOrderId, long makerOrderId,
                                long takerUserId, long makerUserId,
                                OrderSide takerSide, long price, long quantity,
                                int priceScale, int quantityScale, int quoteScale) {
        long amount = Amounts.quoteAmount(price, quantity, priceScale, quantityScale, quoteScale);
        return new Trade(tradeId, symbol, takerOrderId, makerOrderId, takerUserId,
                makerUserId, takerSide, price, quantity, amount);
    }

    /** 兼容无精度信息的调用：金额按 {@code price × quantity} 的原始乘积处理。 */
    public static Trade matched(long tradeId, String symbol,
                                long takerOrderId, long makerOrderId,
                                long takerUserId, long makerUserId,
                                OrderSide takerSide, long price, long quantity) {
        return matched(tradeId, symbol, takerOrderId, makerOrderId, takerUserId,
                makerUserId, takerSide, price, quantity, 0, 0, 0);
    }
}
