package com.me.currency.vo;

import java.util.List;

/**
 * 用户历史资金流水查询结果。
 *
 * <p>成功时 {@code flows} 按发生时间倒序（最新记录在前）；失败时 {@code reason} 是错误码，
 * {@code flows} 为空列表。{@code userId} / {@code currency} 回显本次实际生效的筛选条件。
 */
public record UserFundsFlowQueryView(
        boolean success,
        String reason,
        Long userId,
        String currency,
        List<UserFundsFlowView> flows) {

    public UserFundsFlowQueryView {
        flows = flows == null ? List.of() : List.copyOf(flows);
    }

    public static UserFundsFlowQueryView success(Long userId, String currency,
                                                 List<UserFundsFlowView> flows) {
        return new UserFundsFlowQueryView(true, null, userId, currency, flows);
    }

    public static UserFundsFlowQueryView rejected(Long userId, String currency, String reason) {
        return new UserFundsFlowQueryView(false, reason, userId, currency, List.of());
    }
}
