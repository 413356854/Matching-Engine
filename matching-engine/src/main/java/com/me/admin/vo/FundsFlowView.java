package com.me.admin.vo;

import com.me.admin.entity.FundsFlowRow;
import java.time.Instant;

/**
 * 资金流水视图：对外暴露的一条流水快照。
 *
 * <p>{@code amount} 及各余额字段都是币种最小单位的整数；{@code logType} 是日志类型编码，
 * {@code op} 是它对应的原子余额操作；{@code logDesc} 是中文描述。
 */
public record FundsFlowView(
        String eventId,
        String requestId,
        long userId,
        String currency,
        String logType,
        String op,
        String logDesc,
        long amount,
        long availableBefore,
        long availableAfter,
        long frozenBefore,
        long frozenAfter,
        long seq,
        Instant occurredAt) {

    public static FundsFlowView from(FundsFlowRow row) {
        return new FundsFlowView(
                row.eventId(),
                row.requestId(),
                row.userId(),
                row.currency(),
                row.logType().type(),
                row.op().name(),
                row.logDesc(),
                row.amount(),
                row.availableBefore(),
                row.availableAfter(),
                row.frozenBefore(),
                row.frozenAfter(),
                row.seq(),
                row.occurredAt());
    }
}
