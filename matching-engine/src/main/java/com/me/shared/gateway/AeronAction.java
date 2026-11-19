package com.me.shared.gateway;

/**
 * WebSocket 交易端点通过 Aeron 发给币币模块的请求类型。
 */
public enum AeronAction {

    /** 下单：字段含义与 {@code PlaceOrderRequest} 一致，{@code orderId} 为空。 */
    PLACE,

    /** 撤单：{@code orderId} / {@code symbol} 必填，{@code userId} 来自连接登录态。 */
    CANCEL
}
