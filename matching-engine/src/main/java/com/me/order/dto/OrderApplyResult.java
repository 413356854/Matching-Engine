package com.me.order.dto;

import com.me.account.dto.FundsCommand;
import com.me.order.vo.OrderView;

/**
 * 委托命令执行结果：成功时携带保存后的委托视图和需要发送的冻结命令，
 * 失败时携带拒绝原因。
 *
 * <p>{@code freezeCommand} 只在委托处于 {@code ACCEPTED}（等待冻结回执）时非空；
 * 已有冻结结果（已支付 / 已拒绝）的委托不会再触发冻结，值为 {@code null}。
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
