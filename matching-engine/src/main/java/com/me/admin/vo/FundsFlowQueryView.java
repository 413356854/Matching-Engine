package com.me.admin.vo;

import java.util.List;

/**
 * 管理端流水查询结果。
 *
 * <p>成功时 {@code flows} 按发生时间倒序（最新在前）；失败时 {@code reason} 是错误码，
 * {@code flows} 为空列表。{@code userId} / {@code currency} / {@code logType} 回显本次
 * 实际生效的筛选条件，未筛选时为 {@code null}。
 */
public record FundsFlowQueryView(
        boolean success,
        String reason,
        Long userId,
        String currency,
        String logType,
        List<FundsFlowView> flows) {

    public FundsFlowQueryView {
        flows = flows == null ? List.of() : List.copyOf(flows);
    }

    public static FundsFlowQueryView success(Long userId, String currency, String logType,
                                             List<FundsFlowView> flows) {
        return new FundsFlowQueryView(true, null, userId, currency, logType, flows);
    }

    public static FundsFlowQueryView rejected(Long userId, String currency, String logType,
                                              String reason) {
        return new FundsFlowQueryView(false, reason, userId, currency, logType, List.of());
    }
}
