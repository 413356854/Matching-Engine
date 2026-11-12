package com.me.admin.vo;

/**
 * 管理端一键清空流水结果。
 *
 * <p>成功时 {@code deletedCount} 是清空前表中的记录数；失败时 {@code reason} 是错误码。
 */
public record FundsFlowClearView(boolean success, String reason, long deletedCount) {

    public static FundsFlowClearView success(long deletedCount) {
        return new FundsFlowClearView(true, null, deletedCount);
    }

    public static FundsFlowClearView rejected(String reason) {
        return new FundsFlowClearView(false, reason, 0);
    }
}
