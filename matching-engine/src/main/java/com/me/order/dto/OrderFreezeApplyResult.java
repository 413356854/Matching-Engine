package com.me.order.dto;

import com.me.shared.order.OrderPlacedEvent;

/**
 * 冻结回执处理结论。
 *
 * <p>{@code handled=false} 表示回执没有对应的委托或与委托不匹配，可以安全忽略；
 * {@code handled=true} 且 {@code placedEvent != null} 表示委托已支付，需要交给进程内撮合；
 * {@code handled=true} 且 {@code placedEvent == null} 表示委托已被冻结失败拒绝。
 *
 * <p>委托已经是 {@code PAID} / {@code PARTIALLY_FILLED} / {@code FILLED} 时，重复的冻结成功回执
 * 也会返回 {@code placedEvent}；撮合引擎按 orderId 去重，重复回执不会再次产生行情/结算。
 */
public record OrderFreezeApplyResult(
        boolean handled,
        String reason,
        OrderPlacedEvent placedEvent) {

    /** 冻结成功，委托已支付，交给进程内撮合。 */
    public static OrderFreezeApplyResult placed(OrderPlacedEvent event) {
        return new OrderFreezeApplyResult(true, null, event);
    }

    /** 冻结失败，委托已拒绝。 */
    public static OrderFreezeApplyResult rejected(String reason) {
        return new OrderFreezeApplyResult(true, reason, null);
    }

    /** 回执与委托无关（委托不存在、requestId 不匹配或状态已终结），忽略。 */
    public static OrderFreezeApplyResult ignored(String reason) {
        return new OrderFreezeApplyResult(false, reason, null);
    }
}
