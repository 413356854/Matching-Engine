package com.me.currency.vo;

/**
 * 币币模块下单回执。
 *
 * <p>成功只表示委托命令已进入 Kafka producer 的异步发送缓冲区，不表示命令已到达 broker，
 * 更不表示委托已创建或已冻结；委托保存、冻结回执和撮合结果全部由委托模块异步推进。
 */
public record PlaceOrderSubmissionView(
        boolean success,
        String reason,
        String requestId,
        Long orderId,
        String symbol) {

    public static PlaceOrderSubmissionView accepted(String requestId, long orderId, String symbol) {
        return new PlaceOrderSubmissionView(true, null, requestId, orderId, symbol);
    }

    public static PlaceOrderSubmissionView rejected(String requestId, String reason) {
        return new PlaceOrderSubmissionView(false, reason, requestId, null, null);
    }

    public static PlaceOrderSubmissionView rejected(String requestId, long orderId, String symbol,
                                                   String reason) {
        return new PlaceOrderSubmissionView(false, reason, requestId, orderId, symbol);
    }
}
