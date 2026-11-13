package com.me.currency.dto;

/**
 * 币币模块撤单请求。
 *
 * <p>{@code symbol} 与 {@code orderId} 由前端提供：{@code orderId} 定位待撤销的委托，
 * {@code symbol} 既用于交易对校验，也作为 Kafka key 保证同一交易对的撤单按序处理。
 *
 * <p>{@code requestId} 是撤单请求标识，缺省时由服务端生成；{@code userId} 可选，
 * 传入时委托模块会校验委托归属（0 或省略表示不校验）。撤单接口只做同步校验并投递命令，
 * 真正的撤销结果由委托模块异步推进，可通过 {@code GET /api/v1/orders} 观察。
 */
public record CancelOrderRequest(
        String requestId,
        Long orderId,
        String symbol,
        Long userId) {
}
