package com.me.websocket.trading.vo;

/**
 * /ws/trade 服务端消息。
 *
 * <p>消息类型：
 * <ul>
 *   <li>{@code loggedIn}：连接登录成功；</li>
 *   <li>{@code orderAck}：币币模块的 ingress ack，{@code stage=INGRESS} 表示命令已进入
 *       ingress ring，不代表委托已受理 / 冻结 / 成交；</li>
 *   <li>{@code error}：WS 层或传输层错误；</li>
 *   <li>{@code orderAckTimeout}：等待 ack 超时，客户端应查单，不应自动重试；</li>
 *   <li>{@code pong}：心跳响应。</li>
 * </ul>
 */
public record TradeServerMessage(
        String type,
        String stage,
        Long userId,
        String requestId,
        Boolean success,
        String reason,
        Long orderId,
        String symbol) {

    public static TradeServerMessage loggedIn(long userId) {
        return new TradeServerMessage("loggedIn", null, userId, null, null, null, null, null);
    }

    public static TradeServerMessage orderAck(String requestId, boolean success, String reason,
                                              Long orderId, String symbol) {
        return new TradeServerMessage(
                "orderAck", "INGRESS", null, requestId, success, reason, orderId, symbol);
    }

    public static TradeServerMessage error(String requestId, String reason) {
        return new TradeServerMessage(
                "error", null, null, requestId, null, reason, null, null);
    }

    public static TradeServerMessage ackTimeout(String requestId) {
        return new TradeServerMessage(
                "orderAckTimeout", null, null, requestId, null, null, null, null);
    }

    public static TradeServerMessage pong() {
        return new TradeServerMessage("pong", null, null, null, null, null, null, null);
    }
}
