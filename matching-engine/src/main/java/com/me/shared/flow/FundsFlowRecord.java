package com.me.shared.flow;

import com.me.shared.event.FundsLogType;
import com.me.shared.event.FundsOp;
import java.time.Instant;

/**
 * 用户历史资金流水（QuestDB {@code funds_flow} 查询投影的只读快照）。
 *
 * <p>由 admin 模块的 QuestDB 访问层查询后映射，币币模块只读取该契约，不直接访问
 * {@code funds_flow} 表。金额与余额都是币种最小单位的整数；{@code occurredAt} 是
 * 账户操作发生时间，查询结果按它倒序（最新在前）。
 */
public record FundsFlowRecord(
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
