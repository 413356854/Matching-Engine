package com.me.admin.mq;

import com.me.admin.service.OrderHistoryArchiveService;
import com.me.shared.order.OrderHistoryBatch;
import com.me.shared.order.OrderHistoryEvent;
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
 * 消费 {@code me.order.history-batch}，把撤单 / 撮合完成的委托批量归档到 QuestDB。
 *
 * <p><b>批量消费与批量写入</b>：{@code batch="true"}，一个 poll 批次一个 Kafka 事务；poll 到的
 * 多条 record 先全部展开，再按 {@link #INSERT_BATCH_SIZE} 行分块写入 QuestDB，摊薄 QWP
 * flush + ack 的固定开销。
 *
 * <p><b>幂等</b>：QuestDB {@code ord_history} 按 {@code (final_at, order_id)} DEDUP，
 * 同一委托重投不会产生重复行；消费端再用有界 orderId 集合跳过已成功归档的事件，
 * 减少重复写入。集合只在整块写入成功后更新。
 */
@Component
public class OrderHistoryConsumer {

    /** 已成功归档的 orderId 窗口；只需覆盖批量重试窗口。 */
    static final int SEEN_ORDER_LIMIT = 65_536;

    /** 单次 QuestDB/QWP 写入的最大委托数。 */
    static final int INSERT_BATCH_SIZE = 20_000;

    private final OrderHistoryArchiveService archiveService;

    /**
     * 已成功写入 QuestDB 的 orderId。必须在整块写入成功后加入，避免写失败的事件在重投时被误跳过。
     *
     * <p>order-history 可以有多个 consumer 并发消费（key=symbol 分区），本集合被多个线程共享，
     * 因此必须同步访问；同一委托只会落在一个 symbol 分区，正常情况下不会跨线程竞争。
     */
    private final Set<Long> seenOrderIds = Collections.synchronizedSet(
            Collections.newSetFromMap(new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                    return size() > SEEN_ORDER_LIMIT;
                }
            }));

    public OrderHistoryConsumer(OrderHistoryArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    public void onOrderHistory(List<ConsumerRecord<String, OrderHistoryBatch>> records) {
        List<OrderHistoryEvent> pending = new ArrayList<>(1024);
        Set<Long> batchOrderIds = new HashSet<>();
        for (ConsumerRecord<String, OrderHistoryBatch> record : records) {
            OrderHistoryBatch message = record.value();
            if (message == null) {
                continue;
            }
            for (OrderHistoryEvent event : message.getMsgList()) {
                if (seenOrderIds.contains(event.orderId())
                        || !batchOrderIds.add(event.orderId())) {
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
     * 写入一个分块。成功后才把该块的 orderId 记入去重窗口；失败向上抛，Kafka 事务回滚，
     * 整批重投时已写入前缀会被跳过（QuestDB DEDUP 也会兜底）。
     */
    private void archive(List<OrderHistoryEvent> events) {
        archiveService.archiveBatch(events);
        events.forEach(event -> seenOrderIds.add(event.orderId()));
    }
}
