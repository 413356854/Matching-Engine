package com.me.admin.vo;

/**
 * 管理员充值结果。
 *
 * <p>成功时 {@code requestId} 可用于查询日志/重试；{@code amount} 是实际写入账本的
 * 最小单位整数，{@code displayAmount} 是请求里的展示金额，{@code scale} 是币种精度。
 * 充值命令异步生效，余额变化以账户查询接口为准。
 */
public record AdminRechargeView(
        boolean success,
        String reason,
        String requestId,
        long userId,
        String currency,
        long amount,
        long displayAmount,
        int scale) {

    public static AdminRechargeView success(String requestId, long userId, String currency,
                                            long amount, long displayAmount, int scale) {
        return new AdminRechargeView(true, null, requestId, userId, currency,
                amount, displayAmount, scale);
    }

    public static AdminRechargeView rejected(String reason) {
        return new AdminRechargeView(false, reason, null, 0, null, 0, 0, 0);
    }

    public static AdminRechargeView rejected(long userId, String currency, String reason) {
        return new AdminRechargeView(false, reason, null, userId, currency, 0, 0, 0);
    }
}
