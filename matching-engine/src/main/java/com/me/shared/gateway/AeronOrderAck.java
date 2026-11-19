package com.me.shared.gateway;

/**
 * 币币模块通过 Aeron 返回给 WebSocket 交易端点的 ingress ack。
 *
 * <p><b>语义</b>：{@code success=true} 只表示请求已通过币币模块同步校验并成功写入
 * 下单 / 撤单 Disruptor（进入 ingress ring），不代表 Kafka ack、委托受理、资金冻结或成交。
 * 后续状态仍通过 {@code GET /api/v1/orders} 或按 requestId 查询接口观察。
 *
 * <p>{@code reason} 复用 REST 下单 / 撤单的错误码；传输层和内部异常使用
 * {@code SERVER_BUSY} / {@code ENGINE_UNAVAILABLE} / {@code INTERNAL_ERROR}。
 */
public record AeronOrderAck(
        int version,
        String requestId,
        long connectionId,
        boolean success,
        String reason,
        Long orderId,
        String symbol) {

    /** 当前协议版本。 */
    public static final int CURRENT_VERSION = 1;
}
