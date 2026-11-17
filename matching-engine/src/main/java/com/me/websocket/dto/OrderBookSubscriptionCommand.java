package com.me.websocket.dto;

/**
 * 客户端发来的盘口订阅指令。
 *
 * <p>示例：{@code {"action":"subscribe","symbol":"BTC-USDT","depth":10}}；
 * {@code unsubscribe} 只需要 {@code action}。{@code depth} 缺省时使用服务端默认值。
 * 未知字段会被忽略，便于前后端协议向前兼容。
 */
public record OrderBookSubscriptionCommand(String action, String symbol, Integer depth) {
}
