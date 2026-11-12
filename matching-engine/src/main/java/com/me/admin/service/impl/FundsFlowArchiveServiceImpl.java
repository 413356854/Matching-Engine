package com.me.admin.service.impl;

import com.me.admin.repository.FundsFlowRepository;
import com.me.admin.service.FundsFlowArchiveService;
import com.me.shared.event.FundsFlowEvent;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 资金流水归档实现：按 Kafka poll 批次批量写入 QuestDB。
 */
@Service
public class FundsFlowArchiveServiceImpl implements FundsFlowArchiveService {

    private final FundsFlowRepository repository;

    public FundsFlowArchiveServiceImpl(FundsFlowRepository repository) {
        this.repository = repository;
    }

    @Override
    public void archiveBatch(List<FundsFlowEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        repository.insertBatch(events);
    }
}
