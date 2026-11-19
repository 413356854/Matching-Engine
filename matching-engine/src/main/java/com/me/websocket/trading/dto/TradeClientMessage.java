package com.me.websocket.trading.dto;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;

/**
 * /ws/trade 客户端消息。
 *
 * <p>示例：
 * <pre>
 * {"action":"login","userId":1001}
 * {"action":"placeOrder","requestId":"r-1","symbol":"BTC-USDT",
 *  "side":"BUY","type":"LIMIT","price":50000,"quantity":2}
 * {"action":"cancelOrder","requestId":"r-2","orderId":123,"symbol":"BTC-USDT"}
 * </pre>
 *
 * <p>下单 / 撤单的字段级业务校验由币币模块完成；WS 层只校验登录态、requestId 和限流。
 * 未知字段会被忽略，便于协议向前兼容。
 */
public record TradeClientMessage(
        String action,
        Long userId,
        String requestId,
        String symbol,
        OrderSide side,
        OrderType type,
        Long price,
        Long quantity,
        Long amount,
        Long orderId) {
}
