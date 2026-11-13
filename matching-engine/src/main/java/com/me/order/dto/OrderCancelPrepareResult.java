package com.me.order.dto;

/**
 * 撤单准备阶段结果：可撤时携带委托快照，不可撤时携带拒绝原因。
 *
 * <p>只有 {@code PAID}（已冻结、已进入撮合）与 {@code PARTIALLY_FILLED} 的委托可以撤单。
 * {@code ACCEPTED} 仍在冻结回执途中，直接撤销会与冻结回执竞争，因此拒绝并提示稍后重试；
 * 已成交、已撤销、已拒绝的委托按幂等语义返回对应原因，不产生解冻清算。
 */
public record OrderCancelPrepareResult(
        boolean cancellable,
        String reason,
        OrderCancelContext context) {

    public static OrderCancelPrepareResult ready(OrderCancelContext context) {
        return new OrderCancelPrepareResult(true, null, context);
    }

    public static OrderCancelPrepareResult rejected(String reason) {
        return new OrderCancelPrepareResult(false, reason, null);
    }
}
