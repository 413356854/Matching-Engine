package com.me.account.dto;

import com.me.shared.event.FundsFlowEvent;

/**
 * 账户命令执行结果：成功时携带流水事件，失败时携带拒绝原因。
 */
public record AccountApplyResult(boolean success, String reason, FundsFlowEvent flowEvent) {

    public static AccountApplyResult success(FundsFlowEvent event) {
        return new AccountApplyResult(true, null, event);
    }

    public static AccountApplyResult rejected(String reason) {
        return new AccountApplyResult(false, reason, null);
    }
}
