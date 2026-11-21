package com.me.order.dto;

import com.me.account.dto.FundsCommand;
import com.me.order.vo.OrderView;

/**
 * 委托命令执行结果：成功时携带保存后的委托视图和需要发送的冻结命令，
 * 失败时携带拒绝原因。
 *
 * <p>{@code freezeCommand} 只在本次新建委托且处于 {@code ACCEPTED}（等待冻结回执）时非空；
 * 失败时 {@code freezeCommand} 为空。
 */
public record OrderApplyResult(
        boolean success,
        String reason,
        OrderView order,
        FundsCommand freezeCommand) {

    public static OrderApplyResult success(OrderView order, FundsCommand freezeCommand) {
        return new OrderApplyResult(true, null, order, freezeCommand);
    }

    public static OrderApplyResult rejected(String reason) {
        return new OrderApplyResult(false, reason, null, null);
    }
}
