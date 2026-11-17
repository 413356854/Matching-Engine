package com.me.snapshot.recovery;

import com.me.account.dto.AccountOffsetSeq;
import com.me.account.dto.AccountOffsetSeqBatch;
import com.me.kafka.KafkaClientFactory;
import com.me.kafka.KafkaClientProperties;
import com.me.order.dto.OrderOffsetSeq;
import com.me.order.dto.OrderOffsetSeqBatch;
import com.me.snapshot.SnapshotRecoveryException;
import com.me.snapshot.SnapshotRecoveryProperties;
import com.me.snapshot.dto.ReplayCheckpoint;
import com.me.snapshot.dto.SnapshotPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 从两个 offset-seq topic 读取快照切点之后已提交的「输入记录 → 全局 seq」映射。
 *
 * <p>起点来自快照的 {@code replayCheckpoints}，终点是 read_committed 的 LSO。只有与
 * 事务一起提交的映射可见，因此读到的每条映射都对应一个已提交的输入批次。消息按 seq
 * 排序并保留原始批次边界，交给 {@link ReplayApplier} 重放。
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class OffsetSeqReplayReader {

    private static final Logger log = LoggerFactory.getLogger(OffsetSeqReplayReader.class);
    private static final long POLL_MILLIS = 100L;

    private final KafkaClientFactory clientFactory;
    private final KafkaClientProperties kafkaProperties;
    private final SnapshotRecoveryProperties properties;
    private final AdminClient adminClient;

    public OffsetSeqReplayReader(KafkaClientFactory clientFactory,
                                 KafkaClientProperties kafkaProperties,
                                 SnapshotRecoveryProperties properties,
                                 @Qualifier("snapshotAdminClient") AdminClient adminClient) {
        this.clientFactory = clientFactory;
        this.kafkaProperties = kafkaProperties;
        this.properties = properties;
        this.adminClient = adminClient;
    }

    /**
     * 读取两个模块的重放映射。
     *
     * <p>每条映射是「原始输入记录 (topic/partition/offset) → 模块全局 seq」。只读取
     * checkpoint 之后、read_committed LSO 之前的记录：前者保证不会重复读取快照已覆盖的
     * 映射，后者排除未提交事务；已提交事务与 input offset、输出在同一个事务里，因此
     * 读到的映射一定对应已提交的输入批次。
     *
     * <p>seq <= 快照水位的映射会被过滤；一条消息部分跨越水位视为快照不一致直接失败
     * （checkpoint 是事务边界，正常不会出现半条消息）。
     */
    public ReplayMappings read(SnapshotPayload payload) {
        String orderTopic = topic("order-offset-seq");
        String accountTopic = topic("account-offset-seq");
        Set<String> orderInputs = Set.of(
                topic("order-commands"),
                topic("order-freeze-results"),
                topic("order-cancels"));
        Set<String> accountInputs = Set.of(
                topic("account-commands"),
                topic("account-order-freeze-commands"),
                topic("account-settlements"));

        List<ReplayBatch> order = readTopic(orderTopic, OrderOffsetSeqBatch.class,
                orderInputs, payload.orderSeq(), payload.replayCheckpoints());
        List<ReplayBatch> account = readTopic(accountTopic, AccountOffsetSeqBatch.class,
                accountInputs, payload.accountSeq(), payload.replayCheckpoints());
        log.info("offset-seq replay mappings read: orderBatches={}, orderRecords={}, "
                        + "accountBatches={}, accountRecords={}",
                order.size(), order.stream().mapToLong(b -> b.mappings().size()).sum(),
                account.size(), account.stream().mapToLong(b -> b.mappings().size()).sum());
        return new ReplayMappings(order, account);
    }

    /**
     * 读取单个 offset-seq topic 的尾部映射。
     *
     * <p>先用 admin 读取 [logStart, LSO]：checkpoint 落在 logStart 之前说明映射已被
     * retention 删除，落在 LSO 之后说明快照与当前日志不匹配；两种情况都无法完整重放，
     * 直接失败而不是“尽量读”。
     */
    private <T> List<ReplayBatch> readTopic(String topicName,
                                            Class<T> type,
                                            Set<String> allowedInputTopics,
                                            long watermark,
                                            List<ReplayCheckpoint> checkpoints) {
        Map<TopicPartition, Long> starts = new LinkedHashMap<>();
        for (ReplayCheckpoint checkpoint : checkpoints) {
            if (checkpoint.topic().equals(topicName)) {
                starts.put(new TopicPartition(topicName, checkpoint.partition()),
                        checkpoint.nextOffset());
            }
        }
        if (starts.isEmpty()) {
            throw new SnapshotRecoveryException(
                    "snapshot has no replay checkpoint for topic " + topicName);
        }
        Map<TopicPartition, Long> earliest =
                readOffsets(topicName, starts.keySet(), OffsetSpec.earliest());
        Map<TopicPartition, Long> ends =
                readOffsets(topicName, starts.keySet(), OffsetSpec.latest());
        for (Map.Entry<TopicPartition, Long> entry : starts.entrySet()) {
            Long start = earliest.get(entry.getKey());
            Long end = ends.get(entry.getKey());
            if (start == null || end == null) {
                throw new SnapshotRecoveryException("missing offset bounds for "
                        + entry.getKey());
            }
            if (entry.getValue() < start) {
                throw new SnapshotRecoveryException("replay checkpoint " + entry.getKey()
                        + " nextOffset=" + entry.getValue() + " is before the log start "
                        + start + "; offset-seq records were deleted by retention");
            }
            if (entry.getValue() > end) {
                throw new SnapshotRecoveryException("replay checkpoint " + entry.getKey()
                        + " nextOffset=" + entry.getValue() + " is beyond end offset " + end);
            }
        }

        List<ConsumerRecord<String, T>> records =
                readRecords(topicName, type, starts, ends);
        return toBatches(topicName, records, allowedInputTopics, watermark);
    }

    /**
     * 从 checkpoint 逐分区读到 LSO，返回全部可见映射消息。
     *
     * <p>使用独立随机 group + {@code assign()}，不参与消费组协调、不提交 offset，
     * 因此不会影响真正的消费组。进度按 position 前进判断，避免 read_committed 跳过
     * 控制 / aborted 记录时被误判为卡住。
     */
    private <T> List<ConsumerRecord<String, T>> readRecords(String topicName,
                                                            Class<T> type,
                                                            Map<TopicPartition, Long> starts,
                                                            Map<TopicPartition, Long> ends) {
        String group = "snapshot-replay-offset-seq-" + UUID.randomUUID();
        KafkaConsumer<String, T> consumer = clientFactory.createConsumer(
                "snapshot-replay-offset-seq", group, type);
        try {
            Set<TopicPartition> partitions = new HashSet<>(starts.keySet());
            consumer.assign(partitions);
            for (Map.Entry<TopicPartition, Long> entry : starts.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }

            Set<TopicPartition> pending = new HashSet<>();
            for (TopicPartition partition : partitions) {
                if (starts.get(partition) < ends.get(partition)) {
                    pending.add(partition);
                }
            }

            List<ConsumerRecord<String, T>> all = new ArrayList<>();
            Map<TopicPartition, Long> positions = new HashMap<>();
            long lastProgress = System.nanoTime();
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(properties.fetchTimeoutMs());

            while (!pending.isEmpty()) {
                ConsumerRecords<String, T> polled = consumer.poll(Duration.ofMillis(POLL_MILLIS));
                boolean progress = !polled.isEmpty();
                for (ConsumerRecord<String, T> record : polled) {
                    all.add(record);
                    if (all.size() > properties.maxMappings()) {
                        throw new SnapshotRecoveryException("offset-seq replay tail exceeds "
                                + "max-mappings=" + properties.maxMappings()
                                + "; take a newer snapshot");
                    }
                }
                for (TopicPartition partition : pending) {
                    long position = consumer.position(partition);
                    Long previous = positions.put(partition, position);
                    if (previous == null || position > previous) {
                        progress = true;
                    }
                }
                pending.removeIf(partition ->
                        consumer.position(partition) >= ends.get(partition));

                long now = System.nanoTime();
                if (progress) {
                    lastProgress = now;
                }
                if (now - lastProgress > timeoutNanos) {
                    throw new SnapshotRecoveryException("timed out reading " + topicName
                            + " from replay checkpoints; pending partitions=" + pending);
                }
            }
            all.sort(Comparator
                    .comparingInt((ConsumerRecord<String, T> record) -> record.partition())
                    .thenComparingLong(ConsumerRecord::offset));
            return all;
        } catch (SnapshotRecoveryException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SnapshotRecoveryException(
                    "failed to read " + topicName + " for replay: " + e.getMessage(), e);
        } finally {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("failed to close replay mapping consumer for {}", topicName, e);
            }
        }
    }

    private Map<TopicPartition, Long> readOffsets(String topicName,
                                                  Set<TopicPartition> partitions,
                                                  OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (TopicPartition partition : partitions) {
            request.put(partition, spec);
        }
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
                    adminClient.listOffsets(request,
                                    new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                            .all()
                            .get(properties.adminTimeoutMs(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
            result.forEach((partition, info) -> offsets.put(partition, info.offset()));
            return offsets;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SnapshotRecoveryException(
                    "interrupted while reading offsets for " + topicName);
        } catch (ExecutionException | TimeoutException e) {
            throw new SnapshotRecoveryException(
                    "failed to read offsets for " + topicName + ": " + e, e);
        }
    }

    /**
     * 把 offset-seq Kafka 消息转换成重放批次。
     *
     * <p>一条 Kafka 消息就是原模块的一次 service 调用，因此保留批次边界，重放时能复用
     * 相同的批内并发 / 聚合语义。validate 并在转换时完成：
     * <ul>
     *   <li>快照水位过滤（全部在快照之前 → 跳过；部分跨越 → 失败）；</li>
     *   <li>同一批次的输入 topic 必须一致，seq 必须连续；</li>
     *   <li>seq 和输入位置全局唯一，防止重复映射导致重复执行；</li>
     *   <li>按首 seq 排序并检查批次之间不重叠。</li>
     * </ul>
     */
    private List<ReplayBatch> toBatches(String offsetSeqTopic,
                                        List<? extends ConsumerRecord<String, ?>> records,
                                        Set<String> allowedInputTopics,
                                        long watermark) {
        List<ReplayBatch> batches = new ArrayList<>();
        Map<Long, ReplayMapping> bySeq = new HashMap<>();
        Map<InputKey, ReplayMapping> byInput = new HashMap<>();
        long total = 0;

        for (ConsumerRecord<String, ?> record : records) {
            List<ReplayMapping> mappings = extractMappings(record.value(), offsetSeqTopic);
            if (mappings.isEmpty()) {
                continue;
            }

            boolean anyAfter = false;
            boolean anyBefore = false;
            for (ReplayMapping mapping : mappings) {
                if (mapping.seq() > watermark) {
                    anyAfter = true;
                } else {
                    anyBefore = true;
                }
            }
            if (!anyAfter) {
                continue;
            }
            if (anyBefore) {
                throw new SnapshotRecoveryException("offset-seq message crosses the snapshot "
                        + "watermark: topic=" + offsetSeqTopic + ", kafkaPartition="
                        + record.partition() + ", kafkaOffset=" + record.offset()
                        + ", watermark=" + watermark);
            }

            String inputTopic = mappings.get(0).topic();
            for (int index = 0; index < mappings.size(); index++) {
                ReplayMapping mapping = mappings.get(index);
                if (!inputTopic.equals(mapping.topic())) {
                    throw new SnapshotRecoveryException("offset-seq batch mixes input topics: "
                            + offsetSeqTopic + ", first=" + inputTopic
                            + ", actual=" + mapping.topic());
                }
                if (mapping.seq() != mappings.get(0).seq() + index) {
                    throw new SnapshotRecoveryException("offset-seq batch has non-contiguous "
                            + "seq: topic=" + inputTopic + ", partition=" + mapping.partition()
                            + ", first=" + mappings.get(0).seq() + ", actual=" + mapping.seq());
                }
                register(bySeq, byInput, mapping);
            }
            if (!allowedInputTopics.contains(inputTopic)) {
                throw new SnapshotRecoveryException("offset-seq mapping references unexpected "
                        + "input topic: " + inputTopic);
            }

            total += mappings.size();
            if (total > properties.maxMappings()) {
                throw new SnapshotRecoveryException("replay tail exceeds max-mappings="
                        + properties.maxMappings() + "; take a newer snapshot");
            }
            batches.add(new ReplayBatch(inputTopic, mappings));
        }

        batches.sort(Comparator.comparingLong(ReplayBatch::firstSeq));
        long previousLast = Long.MIN_VALUE;
        for (ReplayBatch batch : batches) {
            if (batch.firstSeq() <= previousLast) {
                throw new SnapshotRecoveryException("overlapping offset-seq replay batches: "
                        + "previousLast=" + previousLast + ", nextFirst=" + batch.firstSeq());
            }
            previousLast = batch.lastSeq();
        }
        return batches;
    }

    private List<ReplayMapping> extractMappings(Object value, String offsetSeqTopic) {
        List<?> entries;
        if (value instanceof OrderOffsetSeqBatch orderBatch) {
            entries = orderBatch.getMsgList();
        } else if (value instanceof AccountOffsetSeqBatch accountBatch) {
            entries = accountBatch.getMsgList();
        } else {
            throw new SnapshotRecoveryException("unexpected offset-seq payload type on "
                    + offsetSeqTopic + ": "
                    + (value == null ? "null" : value.getClass().getName()));
        }
        List<ReplayMapping> mappings = new ArrayList<>(entries.size());
        for (Object entry : entries) {
            if (entry instanceof OrderOffsetSeq order) {
                mappings.add(new ReplayMapping(
                        order.topic(), order.partition(), order.offset(), order.orderSeq()));
            } else if (entry instanceof AccountOffsetSeq account) {
                mappings.add(new ReplayMapping(account.topic(), account.partition(),
                        account.offset(), account.accountSeq()));
            } else {
                throw new SnapshotRecoveryException("unexpected offset-seq entry type on "
                        + offsetSeqTopic + ": "
                        + (entry == null ? "null" : entry.getClass().getName()));
            }
        }
        return mappings;
    }

    private void register(Map<Long, ReplayMapping> bySeq,
                          Map<InputKey, ReplayMapping> byInput,
                          ReplayMapping mapping) {
        ReplayMapping sameSeq = bySeq.putIfAbsent(mapping.seq(), mapping);
        if (sameSeq != null) {
            throw new SnapshotRecoveryException("duplicate offset-seq seq " + mapping.seq()
                    + ": existing=" + sameSeq + ", new=" + mapping);
        }
        InputKey key = new InputKey(mapping.topic(), mapping.partition(), mapping.offset());
        ReplayMapping sameInput = byInput.putIfAbsent(key, mapping);
        if (sameInput != null) {
            throw new SnapshotRecoveryException("duplicate offset-seq mapping for input "
                    + key + ": existing=" + sameInput + ", new=" + mapping);
        }
    }

    private String topic(String key) {
        String value = kafkaProperties.topics().get(key);
        if (value == null || value.isBlank()) {
            throw new SnapshotRecoveryException("missing app.kafka.topics." + key);
        }
        return value;
    }

    private record InputKey(String topic, int partition, long offset) {
    }
}
