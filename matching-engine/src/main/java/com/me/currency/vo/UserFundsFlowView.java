package com.me.currency.vo;

import com.me.shared.flow.FundsFlowRecord;
import java.time.Instant;

/**
 * 用户历史资金流水视图：交易页面展示的一条流水快照。
 *
 * <p>{@code amount} 及各余额字段都是币种最小单位的整数；{@code logType} 是日志类型编码，
 * {@code op} 是它对应的原子余额操作；{@code logDesc} 是中文描述。
 */
public record UserFundsFlowView(
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

    public static UserFundsFlowView from(FundsFlowRecord record) {
        return new UserFundsFlowView(
                record.eventId(),
                record.requestId(),
                record.userId(),
                record.currency(),
                record.logType().type(),
                record.op().name(),
                record.logDesc(),
                record.amount(),
                record.availableBefore(),
                record.availableAfter(),
                record.frozenBefore(),
                record.frozenAfter(),
                record.seq(),
                record.occurredAt());
    }
}
