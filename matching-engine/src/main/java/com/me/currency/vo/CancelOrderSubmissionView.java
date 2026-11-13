package com.me.currency.vo;

/**
 * 币币模块撤单回执。
 *
 * <p>成功只表示撤单命令已进入独立的撤单 Disruptor，不表示命令已到达 broker，
 * 更不表示委托已撤销；委托状态推进与解冻清算由委托模块异步完成。
 */
public record CancelOrderSubmissionView(
        boolean success,
        String reason,
        String requestId,
        Long orderId,
        String symbol) {

    public static CancelOrderSubmissionView accepted(String requestId, long orderId,
                                                     String symbol) {
        return new CancelOrderSubmissionView(true, null, requestId, orderId, symbol);
    }

    public static CancelOrderSubmissionView rejected(String requestId, String reason) {
        return new CancelOrderSubmissionView(false, reason, requestId, null, null);
    }

    public static CancelOrderSubmissionView rejected(String requestId, long orderId,
                                                     String symbol, String reason) {
        return new CancelOrderSubmissionView(false, reason, requestId, orderId, symbol);
    }
}
