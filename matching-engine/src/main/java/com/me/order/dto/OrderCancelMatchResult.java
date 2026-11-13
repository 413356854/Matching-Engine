package com.me.order.dto;

import com.me.matching.dto.CancelResult;

/**
 * 一笔撤单在撮合阶段的处理结果。
 *
 * <p>{@code engineResult} 是撮合引擎移除挂单的结果（{@code removed=false} 表示委托当前
 * 不在订单簿中）；{@code accepted} 表示在同一引擎对象锁临界区内成功把委托标记为
 * “撤单中”，该委托可以进入后续的订单更新阶段生成解冻清算。{@code accepted=false} 时
 * {@code reason} 说明原因，调用方跳过该委托。
 */
public record OrderCancelMatchResult(
        OrderCancelContext context,
        CancelResult engineResult,
        boolean accepted,
        String reason) {
}
