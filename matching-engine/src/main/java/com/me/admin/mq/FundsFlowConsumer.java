package com.me.admin.mq;

import com.me.admin.service.FundsFlowArchiveService;
import com.me.shared.event.FundsFlowBatch;
import com.me.shared.event.FundsFlowEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费资金流水批量消息，把一个 poll 批次内的全部流水批量写入 QuestDB。
 *
 * <p><b>批量消费与批量写入</b>：{@code batch="true"}，一个 poll 批次一个 Kafka 事务；poll 到的
 * 多条 record 先全部展开，再按 {@link #INSERT_BATCH_SIZE} 行分块写入 QuestDB（块满即写，
 * poll 结束时写余量）。这样在高吞吐时能摊薄每次 QWP flush+ack 的固定开销，又不会让单次
 * QWP 批次无界增长。
 *
 * <p>QuestDB 写入不在 Kafka 事务里，且 JDBC batch 不是原子操作：如果块中后面的记录失败，
 * 前面已插入的行不会回滚。`funds_flow` 表当前没有 DEDUP，因此这里按 `eventId` 做有界去重——
 * 每个分块写入成功后才记录该块的 eventId，重投时已写入前缀会被跳过、未写入部分重新插入。
 */
@Component
public class FundsFlowConsumer {

    /** 已成功归档的 eventId 窗口；只需覆盖批量重试窗口。 */
    static final int SEEN_EVENT_LIMIT = 65_536;

    /** 单次 QuestDB/QWP 写入的最大流水数；块满即写，避免小批次过多。 */
    static final int INSERT_BATCH_SIZE = 20_000;

    private final FundsFlowArchiveService archiveService;

    /**
     * 已成功写入 QuestDB 的 eventId。必须在整批写入成功后加入，避免写失败的事件在重投时被误跳过。
     *
     * <p>funds-flow 可以有多个 consumer 并发消费（key=userId 分区），本集合被多个线程共享，
     * 因此必须同步访问。同一用户的流水固定在同一分区，正常情况下不会出现跨线程同一 eventId；
     * 这里主要防止 rebalance/重放时两个线程同时读写集合。
     */
    private final Set<String> seenEventIds = Collections.synchronizedSet(
            Collections.newSetFromMap(new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > SEEN_EVENT_LIMIT;
                }
            }));

    public FundsFlowConsumer(FundsFlowArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    public void onFundsFlow(List<ConsumerRecord<String, FundsFlowBatch>> records) {
        List<FundsFlowEvent> pending = new ArrayList<>(1024);
        Set<String> batchEventIds = new HashSet<>();
        for (ConsumerRecord<String, FundsFlowBatch> record : records) {
            FundsFlowBatch message = record.value();
            if (message == null) {
                continue;
            }
            for (FundsFlowEvent event : message.getMsgList()) {
                if (seenEventIds.contains(event.eventId()) || !batchEventIds.add(event.eventId())) {
                    continue;
                }
                pending.add(event);
                if (pending.size() >= INSERT_BATCH_SIZE) {
                    archive(pending);
                    pending = new ArrayList<>(1024);
                }
            }
        }
        if (!pending.isEmpty()) {
            archive(pending);
        }
    }

    /**
     * 写入一个分块。成功后才把该块的 eventId 记入去重窗口；失败向上抛，Kafka 事务回滚，
     * 整批重投时已写入前缀会被跳过，未写入部分重新插入。
     */
    private void archive(List<FundsFlowEvent> events) {
        archiveService.archiveBatch(events);
        events.forEach(event -> seenEventIds.add(event.eventId()));
    }
}
