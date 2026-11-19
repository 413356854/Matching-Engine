package com.me.shared.gateway;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;

/**
 * WebSocket 交易端点发给币币模块的 Aeron 请求。
 *
 * <p>WS 模块只负责把客户端消息转成该结构；币币模块收到后复用
 * {@code OrderCommandAssembler} / {@code OrderCancelAssembler} 做参数校验和委托号生成。
 * {@code userId} 来自 WebSocket 连接的 login 状态，不由单条订单消息决定。
 *
 * <p>{@code replyStreamId} 是 ack 的返回 stream：当前单实例使用配置的 ack stream；
 * 多 WS 实例部署时每个实例使用自己的 stream，由请求携带。
 *
 * <p>下单：{@code side} / {@code orderType} / {@code price} / {@code quantity} /
 * {@code amount} 与 REST {@code PlaceOrderRequest} 相同，{@code orderId} 为空。
 * 撤单：{@code orderId} / {@code symbol} 必填，其余字段可为空。
 */
public record AeronOrderRequest(
        int version,
        AeronAction action,
        String requestId,
        long connectionId,
        long userId,
        int replyStreamId,
        String symbol,
        OrderSide side,
        OrderType orderType,
        Long price,
        Long quantity,
        Long amount,
        Long orderId) {

    /** 当前协议版本；后续字段变更时递增。 */
    public static final int CURRENT_VERSION = 1;
}
