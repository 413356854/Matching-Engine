package com.me.shared.event;

import java.time.Instant;

/**
 * 资金流水事件：account 模块生产，admin 模块消费并归档。
 *
 * <p>金额一律是币种最小单位的 {@code long}，全链路不使用 {@code BigDecimal} / {@code double}。
 * {@code seq} 是同一 (userId, currency) 账户内的单调递增序号。
 *
 * <p>事件只携带日志类型 {@link FundsLogType}，下游通过 {@code logType.op()} 获取原子操作类型。
 */
public record FundsFlowEvent(
        String eventId,
        String requestId,
        long userId,
        String currency,
        FundsLogType logType,
        long amount,
        long availableBefore,
        long availableAfter,
        long frozenBefore,
        long frozenAfter,
        long seq,
        Instant occurredAt) {
}
