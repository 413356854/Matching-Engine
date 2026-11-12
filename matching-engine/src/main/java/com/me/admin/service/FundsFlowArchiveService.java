package com.me.admin.service;

import com.me.shared.event.FundsFlowEvent;
import java.util.List;

/**
 * 资金流水归档服务：把 Kafka 流水批量写入查询存储。
 */
public interface FundsFlowArchiveService {

    /**
     * 批量归档一个 Kafka poll 批次中的流水。
     *
     * @param events 同一批次的事件，按消费顺序
     */
    void archiveBatch(List<FundsFlowEvent> events);

    /** 单条归档便捷方法，等价于只含一条事件的批量归档。 */
    default void archive(FundsFlowEvent event) {
        archiveBatch(List.of(event));
    }
}
