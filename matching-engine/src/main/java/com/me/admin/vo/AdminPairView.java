package com.me.admin.vo;

import com.me.currency.vo.TradingPairView;

/**
 * 管理端交易对写操作结果。
 *
 * <p>成功时 {@code pair} 是写入后的完整配置；失败时 {@code reason} 是错误码。
 */
public record AdminPairView(
        boolean success,
        String reason,
        TradingPairView pair) {

    public static AdminPairView success(TradingPairView pair) {
        return new AdminPairView(true, null, pair);
    }

    public static AdminPairView rejected(String reason) {
        return new AdminPairView(false, reason, null);
    }
}
