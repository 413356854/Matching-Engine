package com.me.marketdata.vo;

/**
 * 管理端一键清空 K 线结果。
 *
 * <p>成功时 {@code deletedCount} 是清空前表中的记录数；失败时 {@code reason} 是错误码。
 * 只清空 {@code md_kline}，成交明细 {@code md_trade} 不受影响。
 */
public record AdminKlineClearView(boolean success, String reason, long deletedCount) {

    public static AdminKlineClearView success(long deletedCount) {
        return new AdminKlineClearView(true, null, deletedCount);
    }

    public static AdminKlineClearView rejected(String reason) {
        return new AdminKlineClearView(false, reason, 0);
    }
}
