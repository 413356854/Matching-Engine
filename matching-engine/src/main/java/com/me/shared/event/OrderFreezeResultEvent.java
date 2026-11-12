package com.me.shared.event;

import java.time.Instant;

/**
 * 下单冻结结果事件：账户模块消费 {@link FundsLogType#ORDER_FREEZE} 命令后回执给委托模块。
 *
 * <p>账户模块把「消费 offset + 资金流水 + 本回执」放在同一个 Kafka 事务里提交，
 * 委托模块据此把委托从 {@code ACCEPTED} 推进到 {@code PAID}（成功）或 {@code REJECTED}（失败）。
 *
 * <p>{@code requestId} 是冻结命令的幂等键（固定为 {@code order-freeze:<orderId>}），
 * {@code orderId} 来自命令的 {@code refId}，两者一起用于把回执关联回委托。
 */
public record OrderFreezeResultEvent(
        String requestId,
        long orderId,
        long userId,
        String currency,
        long amount,
        boolean success,
        String reason,
        Instant occurredAt) {
}
