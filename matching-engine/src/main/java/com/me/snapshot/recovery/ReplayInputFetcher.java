package com.me.snapshot.recovery;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettlementBatch;
import com.me.kafka.KafkaClientFactory;
import com.me.kafka.KafkaClientProperties;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCommandBatch;
import com.me.snapshot.SnapshotRecoveryException;
import com.me.snapshot.SnapshotRecoveryProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 按 offset-seq 映射抓取原始输入记录，并在严格模式下校验重放区间完整。
 *
 * <p>所有 consumer 都使用 {@code assign()}（不 subscribe、不提交 offset），因此不会影响
 * 真正的消费组；isolization level 沿用全局配置（read_committed）。抓取按批次进行，
 * 内存只保留当前批次需要的记录。
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class ReplayInputFetcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReplayInputFetcher.class);
    private static final long POLL_MILLIS = 100L;
    private static final long[] NO_OFFSETS = new long[0];

    private final KafkaClientFactory clientFactory;
    private final KafkaClientProperties kafkaProperties;
    private final SnapshotRecoveryProperties properties;
    private final Map<String, KafkaConsumer<String, Object>> consumers = new HashMap<>();

    public ReplayInputFetcher(KafkaClientFactory clientFactory,
                              KafkaClientProperties kafkaProperties,
                              SnapshotRecoveryProperties properties) {
        this.clientFactory = clientFactory;
        this.kafkaProperties = kafkaProperties;
        this.properties = properties;
    }

    /**
     * 抓取一个重放批次引用的全部输入记录，返回顺序与 {@code mappings} 一致。
     *
     * <p>映射里已经写明每条记录的 (partition, offset)，所以这里按分区 seek 到最小
     * 需要 offset，再 poll 到全部目标 offset 都拿到。这样：
     * <ul>
     *   <li>不需要把整个尾部缓存到内存，内存只保留当前批次；</li>
     *   <li>读不到某个映射 offset（被 retention 删除）会立即失败，而不是静默跳过；</li>
     *   <li>read_committed 可能跳过 aborted / 控制记录，position 前进也算进度，
     *       不会误判超时。</li>
     * </ul>
     *
     * <p>返回的记录保持映射顺序，service 据此重新分配 seq，才能和原始运行一致。
     */
    public List<ConsumerRecord<String, ?>> fetch(String topic, List<ReplayMapping> mappings) {
        if (mappings.isEmpty()) {
            return List.of();
        }
        Map<Integer, List<Long>> offsetsByPartition = new TreeMap<>();
        for (ReplayMapping mapping : mappings) {
            offsetsByPartition.computeIfAbsent(mapping.partition(),
                    ignored -> new ArrayList<>()).add(mapping.offset());
        }
        offsetsByPartition.values().forEach(Collections::sort);

        KafkaConsumer<String, Object> consumer = consumer(topic);
        Set<TopicPartition> partitions = new HashSet<>();
        for (Integer partition : offsetsByPartition.keySet()) {
            partitions.add(new TopicPartition(topic, partition));
        }
        consumer.assign(partitions);
        for (Map.Entry<Integer, List<Long>> entry : offsetsByPartition.entrySet()) {
            consumer.seek(new TopicPartition(topic, entry.getKey()), entry.getValue().get(0));
        }

        Map<RecordPosition, ConsumerRecord<String, Object>> found = new HashMap<>();
        Map<Integer, Long> positions = new HashMap<>();
        Set<Integer> complete = new HashSet<>();
        long lastProgress = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(properties.fetchTimeoutMs());

        while (complete.size() < offsetsByPartition.size()) {
            ConsumerRecords<String, Object> polled =
                    consumer.poll(Duration.ofMillis(POLL_MILLIS));
            boolean progress = !polled.isEmpty();
            for (ConsumerRecord<String, Object> record : polled) {
                List<Long> needed = offsetsByPartition.get(record.partition());
                if (needed != null && Collections.binarySearch(needed, record.offset()) >= 0) {
                    found.put(new RecordPosition(record.partition(), record.offset()), record);
                    progress = true;
                }
            }

            for (Map.Entry<Integer, List<Long>> entry : offsetsByPartition.entrySet()) {
                int partition = entry.getKey();
                if (complete.contains(partition)) {
                    continue;
                }
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                long position = consumer.position(topicPartition);
                Long previous = positions.put(partition, position);
                if (previous == null || position > previous) {
                    // read_committed 可能跳过控制 / aborted 记录，position 前进也算进度。
                    progress = true;
                }
                if (position > entry.getValue().get(entry.getValue().size() - 1)) {
                    complete.add(partition);
                }
                for (Long offset : entry.getValue()) {
                    if (offset < position
                            && !found.containsKey(new RecordPosition(partition, offset))) {
                        throw new SnapshotRecoveryException("input record is missing: topic="
                                + topic + ", partition=" + partition + ", offset=" + offset
                                + "; records were likely deleted by retention");
                    }
                }
            }

            long now = System.nanoTime();
            if (progress) {
                lastProgress = now;
            }
            if (now - lastProgress > timeoutNanos) {
                throw new SnapshotRecoveryException("timed out fetching input records from "
                        + topic + "; pending partitions=" + pending(offsetsByPartition, complete));
            }
        }

        List<ConsumerRecord<String, ?>> ordered = new ArrayList<>(mappings.size());
        for (ReplayMapping mapping : mappings) {
            ConsumerRecord<String, Object> record = found.get(
                    new RecordPosition(mapping.partition(), mapping.offset()));
            if (record == null) {
                throw new SnapshotRecoveryException("input record was not fetched: topic="
                        + topic + ", partition=" + mapping.partition()
                        + ", offset=" + mapping.offset());
            }
            ordered.add(record);
        }
        return ordered;
    }

    /**
     * 严格校验：扫描每个分区 [snapshotOffset, brokerOffset) 内的全部可见记录，要求它们
     * 与 offset-seq 映射一一对应。
     *
     * <p>判断方式：
     * <ul>
     *   <li>每条可见记录都必须在 expectedOffsets 里（否则状态会漏掉这条输入）；</li>
     *   <li>可见记录总数必须等于映射数（否则有映射指向已删除的数据）；</li>
     *   <li>aborted / 控制记录对 read_committed 不可见，不计入任何一边。</li>
     * </ul>
     * 用「计数 + 二分查找」而不是保存可见 offset 集合，避免尾部很大时再复制一份 Set。
     *
     * @param expectedOffsets 每个分区的映射 offset，必须升序
     */
    public void validateCoverage(String topic,
                                 Map<Integer, PartitionRange> ranges,
                                 Map<Integer, long[]> expectedOffsets) {
        if (ranges.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, PartitionRange> entry : ranges.entrySet()) {
            PartitionRange range = entry.getValue();
            long[] expected = expectedOffsets.getOrDefault(entry.getKey(), NO_OFFSETS);
            if (expected.length > 0 && !isSorted(expected)) {
                throw new SnapshotRecoveryException(
                        "internal error: expected offsets are not sorted for " + topic
                                + "/" + entry.getKey());
            }
        }

        KafkaConsumer<String, Object> consumer = consumer(topic);
        Set<TopicPartition> partitions = new HashSet<>();
        for (Integer partition : ranges.keySet()) {
            partitions.add(new TopicPartition(topic, partition));
        }
        try {
            consumer.assign(partitions);
            for (Map.Entry<Integer, PartitionRange> entry : ranges.entrySet()) {
                consumer.seek(new TopicPartition(topic, entry.getKey()),
                        entry.getValue().start());
            }

            Map<Integer, Long> seenCounts = new HashMap<>();
            Map<Integer, Long> extraOffsets = new LinkedHashMap<>();
            Map<Integer, Long> positions = new HashMap<>();
            long lastProgress = System.nanoTime();
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(properties.fetchTimeoutMs());

            while (true) {
                boolean allDone = true;
                for (Map.Entry<Integer, PartitionRange> entry : ranges.entrySet()) {
                    TopicPartition topicPartition = new TopicPartition(topic, entry.getKey());
                    if (consumer.position(topicPartition) < entry.getValue().end()) {
                        allDone = false;
                        break;
                    }
                }
                if (allDone) {
                    break;
                }

                ConsumerRecords<String, Object> polled =
                        consumer.poll(Duration.ofMillis(POLL_MILLIS));
                boolean progress = !polled.isEmpty();
                for (ConsumerRecord<String, Object> record : polled) {
                    PartitionRange range = ranges.get(record.partition());
                    if (range == null
                            || record.offset() < range.start()
                            || record.offset() >= range.end()) {
                        continue;
                    }
                    seenCounts.merge(record.partition(), 1L, Long::sum);
                    long[] expected = expectedOffsets.getOrDefault(
                            record.partition(), NO_OFFSETS);
                    if (Arrays.binarySearch(expected, record.offset()) < 0) {
                        extraOffsets.putIfAbsent(record.partition(), record.offset());
                    }
                    progress = true;
                }
                for (Integer partition : ranges.keySet()) {
                    long position = consumer.position(new TopicPartition(topic, partition));
                    Long previous = positions.put(partition, position);
                    if (previous == null || position > previous) {
                        progress = true;
                    }
                }

                long now = System.nanoTime();
                if (progress) {
                    lastProgress = now;
                }
                if (now - lastProgress > timeoutNanos) {
                    throw new SnapshotRecoveryException("timed out validating replay coverage "
                            + "for " + topic + "; ranges=" + ranges);
                }
            }

            for (Map.Entry<Integer, PartitionRange> entry : ranges.entrySet()) {
                int partition = entry.getKey();
                Long extra = extraOffsets.get(partition);
                if (extra != null) {
                    throw new SnapshotRecoveryException("visible input record has no offset-seq "
                            + "mapping: topic=" + topic + ", partition=" + partition
                            + ", offset=" + extra);
                }
                long[] expected = expectedOffsets.getOrDefault(partition, NO_OFFSETS);
                long seen = seenCounts.getOrDefault(partition, 0L);
                if (seen != expected.length) {
                    throw new SnapshotRecoveryException("replay coverage mismatch: topic="
                            + topic + ", partition=" + partition + ", visibleRecords=" + seen
                            + ", mappedRecords=" + expected.length + ", range=["
                            + entry.getValue().start() + ", " + entry.getValue().end() + ")");
                }
            }
            log.info("replay coverage validated: topic={}, partitions={}", topic, ranges.size());
        } catch (SnapshotRecoveryException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SnapshotRecoveryException("failed to validate replay coverage for "
                    + topic + ": " + e.getMessage(), e);
        }
    }

    private static boolean isSorted(long[] values) {
        for (int index = 1; index < values.length; index++) {
            if (values[index] < values[index - 1]) {
                return false;
            }
        }
        return true;
    }

    private static List<Integer> pending(Map<Integer, List<Long>> offsetsByPartition,
                                         Set<Integer> complete) {
        List<Integer> pending = new ArrayList<>();
        for (Integer partition : offsetsByPartition.keySet()) {
            if (!complete.contains(partition)) {
                pending.add(partition);
            }
        }
        return pending;
    }

    private KafkaConsumer<String, Object> consumer(String topic) {
        return consumers.computeIfAbsent(topic, this::createConsumer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaConsumer<String, Object> createConsumer(String topic) {
        String group = "snapshot-replay-input-" + UUID.randomUUID();
        String client = "snapshot-replay-input-" + topic.replaceAll("[^A-Za-z0-9._-]", "-");
        return (KafkaConsumer) clientFactory.createConsumer(client, group, valueType(topic));
    }

    private Class<?> valueType(String topic) {
        Map<String, String> topics = kafkaProperties.topics();
        if (topic.equals(topics.get("order-commands"))) {
            return OrderCommandBatch.class;
        }
        if (topic.equals(topics.get("order-freeze-results"))) {
            return OrderFreezeResultBatch.class;
        }
        if (topic.equals(topics.get("order-cancels"))) {
            return OrderCancelBatch.class;
        }
        if (topic.equals(topics.get("account-commands"))) {
            return FundsCommand.class;
        }
        if (topic.equals(topics.get("account-order-freeze-commands"))) {
            return OrderFreezeCommandsBatch.class;
        }
        if (topic.equals(topics.get("account-settlements"))) {
            return TradeSettlementBatch.class;
        }
        throw new SnapshotRecoveryException("unsupported replay input topic: " + topic);
    }

    @Override
    public void close() {
        for (Map.Entry<String, KafkaConsumer<String, Object>> entry : consumers.entrySet()) {
            try {
                entry.getValue().close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("failed to close replay input consumer for {}",
                        entry.getKey(), e);
            }
        }
        consumers.clear();
    }

    /** 输入分区待校验区间 [start, end)。 */
    public record PartitionRange(long start, long end) {

        public PartitionRange {
            if (start < 0 || end < start) {
                throw new IllegalArgumentException("invalid partition range: [" + start
                        + ", " + end + ")");
            }
        }
    }

    private record RecordPosition(int partition, long offset) {
    }
}
