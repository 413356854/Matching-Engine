package com.me.admin.dto;

import com.me.currency.vo.TradingPairView;

/**
 * 管理端交易对写操作结果。
 *
 * <p>成功时 {@code pair} 是写入后重新查询的配置；失败时 {@code reason} 是错误码，
 * {@code pair} 为 {@code null}。
 */
public record AdminPairResult(boolean success, String reason, TradingPairView pair) {

    public static AdminPairResult ok(TradingPairView pair) {
        return new AdminPairResult(true, null, pair);
    }

    public static AdminPairResult rejected(String reason) {
        return new AdminPairResult(false, reason, null);
    }
}
