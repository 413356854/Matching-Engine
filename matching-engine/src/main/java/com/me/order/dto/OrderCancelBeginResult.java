package com.me.order.dto;

/**
 * 撤单开始标记结果。
 *
 * <p>由委托模块在<b>引擎对象锁内</b>调用 {@code OrderService.beginCancel}：在移除挂单的同一
 * 临界区里把委托标记为“撤单中”，使并发到达的新撮合在
 * {@code OrderMatchingService.matchBatch} 中看到标记后不再挂单。真正的状态推进
 * （{@code CANCELED} + 解冻清算）仍由后续的订单更新线程池完成。
 *
 * <p>{@code accepted=true} 表示委托可继续撤单（可能已经在撤单中，重试幂等）；
 * {@code accepted=false} 时 {@code reason} 说明原因（已成交、已撤销、不存在等）。
 */
public record OrderCancelBeginResult(
        boolean accepted,
        String reason) {

    public static OrderCancelBeginResult allowed() {
        return new OrderCancelBeginResult(true, null);
    }

    public static OrderCancelBeginResult rejected(String reason) {
        return new OrderCancelBeginResult(false, reason);
    }
}
