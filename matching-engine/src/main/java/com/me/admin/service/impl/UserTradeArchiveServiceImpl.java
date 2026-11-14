package com.me.admin.service.impl;

import com.me.admin.repository.UserTradeRepository;
import com.me.admin.service.UserTradeArchiveService;
import com.me.shared.event.MatchPairEvent;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 用户成交记录归档实现：按 Kafka poll 批次批量写入 QuestDB。
 */
@Service
public class UserTradeArchiveServiceImpl implements UserTradeArchiveService {

    private final UserTradeRepository repository;

    public UserTradeArchiveServiceImpl(UserTradeRepository repository) {
        this.repository = repository;
    }

    @Override
    public void archiveBatch(List<MatchPairEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        repository.insertBatch(events);
    }
}
