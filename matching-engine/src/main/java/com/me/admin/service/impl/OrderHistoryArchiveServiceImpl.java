package com.me.admin.service.impl;

import com.me.admin.repository.OrderHistoryRepository;
import com.me.admin.service.OrderHistoryArchiveService;
import com.me.shared.order.OrderHistoryEvent;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 委托历史归档实现：按 Kafka poll 批次批量写入 QuestDB。
 */
@Service
public class OrderHistoryArchiveServiceImpl implements OrderHistoryArchiveService {

    private final OrderHistoryRepository repository;

    public OrderHistoryArchiveServiceImpl(OrderHistoryRepository repository) {
        this.repository = repository;
    }

    @Override
    public void archiveBatch(List<OrderHistoryEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        repository.insertBatch(events);
    }
}
