package com.me.currency.dto;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;

/**
 * 币币模块下单请求。
 *
 * <p>该接口只做同步校验并投递 Kafka 委托命令，不同步创建委托；请求参数与
 * {@code me.order.commands-batch} 的消息契约保持一致。
 *
 * <p>{@code requestId} 是幂等键，缺省时由服务端生成；{@code orderId} 缺省时由服务端生成，
 * 显式传入时直接使用（压测 / 对账场景）。价格和数量都是交易对最小单位的整数：限价单
 * {@code price} 必填且大于 0，市价单 {@code price} 省略或传 0；{@code quantity} 必填且大于 0。
 *
 * <p>{@code amount} 只用于市价买入：前端用卖一价（没有卖一则用最新成交价）× 1.05 × 数量
 * 算出的计价币预算（计价币最小单位）。其他委托类型省略或传 0。
 */
public record PlaceOrderRequest(
        String requestId,
        Long orderId,
        Long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        Long price,
        Long quantity,
        Long amount) {

    /** 兼容不携带市价买入预算的调用方。 */
    public PlaceOrderRequest(String requestId, Long orderId, Long userId, String symbol,
                             OrderSide side, OrderType type, Long price, Long quantity) {
        this(requestId, orderId, userId, symbol, side, type, price, quantity, null);
    }
}
