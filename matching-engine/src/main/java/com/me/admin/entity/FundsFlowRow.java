package com.me.admin.entity;

import com.me.shared.event.FundsLogType;
import com.me.shared.event.FundsOp;
import java.time.Instant;

/**
 * QuestDB {@code funds_flow} 表的查询结果行。
 */
public record FundsFlowRow(
        String eventId,
        String requestId,
        long userId,
        String currency,
        FundsLogType logType,
        FundsOp op,
        String logDesc,
        long amount,
        long availableBefore,
        long availableAfter,
        long frozenBefore,
        long frozenAfter,
        long seq,
        Instant occurredAt) {
}
