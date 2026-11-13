package com.me.admin.service;

import com.me.shared.order.OrderHistoryEvent;
import java.util.List;

/**
 * 委托历史归档：把委托模块发来的终态事件写入 QuestDB {@code ord_history}。
 */
public interface OrderHistoryArchiveService {

    /** 批量归档一个 Kafka poll 批次中的委托历史；DEDUP 键保证重投不重复。 */
    void archiveBatch(List<OrderHistoryEvent> events);
}
