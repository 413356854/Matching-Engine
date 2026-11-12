package com.me.admin.service.impl;

import com.me.admin.entity.FundsFlowRow;
import com.me.admin.repository.FundsFlowRepository;
import com.me.admin.service.FundsFlowQueryService;
import com.me.shared.event.FundsLogType;
import com.me.shared.flow.FundsFlowHistoryQueryPort;
import com.me.shared.flow.FundsFlowRecord;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 资金流水查询与清理实现：直接委托 QuestDB 访问层。
 *
 * <p>同时实现 shared 的 {@link FundsFlowHistoryQueryPort}，供币币模块的交易页面查询
 * 当前用户的历史流水；查询结果同样按 {@code occurred_at} 倒序。
 */
@Service
public class FundsFlowQueryServiceImpl
        implements FundsFlowQueryService, FundsFlowHistoryQueryPort {

    private final FundsFlowRepository repository;

    public FundsFlowQueryServiceImpl(FundsFlowRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<FundsFlowRow> list(Long userId, String currency, FundsLogType logType, int limit) {
        return repository.findFlows(userId, currency, logType, limit);
    }

    @Override
    public List<FundsFlowRecord> list(long userId, String currency, int limit) {
        return repository.findFlows(userId, currency, null, limit).stream()
                .map(FundsFlowQueryServiceImpl::toRecord)
                .toList();
    }

    @Override
    public long clearAll() {
        return repository.truncateAll();
    }

    private static FundsFlowRecord toRecord(FundsFlowRow row) {
        return new FundsFlowRecord(
                row.eventId(),
                row.requestId(),
                row.userId(),
                row.currency(),
                row.logType(),
                row.op(),
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
