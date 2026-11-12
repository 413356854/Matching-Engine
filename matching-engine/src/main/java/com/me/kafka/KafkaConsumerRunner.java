package com.me.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApplicationRecoverableException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TransactionAbortableException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一条消费链路的裸 kafka-clients 运行循环。
 *
 * <p>语义与原来的 Spring Kafka 批量监听器一致：一个 poll 批次一个事务，输出消息与消费 offset
 * 通过 {@link KafkaProducer#sendOffsetsToTransaction} 原子提交；任何可重试失败都 abort、把
 * 消费位置 reset 回本批第一条、退避后整批重投。
 *
 * <p>事务提交方式参考 Kafka 官方
 * {@code TransactionalClientDemo}：{@code initTransactions} 一次 → {@code beginTransaction}
 * → {@code send}（异步）→ {@code sendOffsetsToTransaction(offsets, consumer.groupMetadata())}
 * → {@code commitTransaction}。
 */
public final class KafkaConsumerRunner<T> implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerRunner.class);

    private final String name;
    private final String topic;
    private final String group;
    private final Class<T> type;
    private final KafkaBatchProcessor<T> processor;
    private final KafkaSink sink;
    private final KafkaClientProperties properties;
    private final KafkaClientFactory clientFactory;
    private final int maxPollRecords;
    private final int fetchMaxWaitMs;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private volatile KafkaConsumer<String, T> consumer;
    private volatile KafkaProducer<String, Object> producer;

    public KafkaConsumerRunner(String name,
                               String topic,
                               String group,
                               Class<T> type,
                               KafkaBatchProcessor<T> processor,
                               KafkaSink sink,
                               KafkaClientProperties properties,
                               KafkaClientFactory clientFactory) {
        this(name, topic, group, type, processor, sink, properties, clientFactory,
                properties.consumer().maxPollRecords(), properties.consumer().fetchMaxWaitMs());
    }

    public KafkaConsumerRunner(String name,
                               String topic,
                               String group,
                               Class<T> type,
                               KafkaBatchProcessor<T> processor,
                               KafkaSink sink,
                               KafkaClientProperties properties,
                               KafkaClientFactory clientFactory,
                               int maxPollRecords) {
        this(name, topic, group, type, processor, sink, properties, clientFactory,
                maxPollRecords, properties.consumer().fetchMaxWaitMs());
    }

    public KafkaConsumerRunner(String name,
                               String topic,
                               String group,
                               Class<T> type,
                               KafkaBatchProcessor<T> processor,
                               KafkaSink sink,
                               KafkaClientProperties properties,
                               KafkaClientFactory clientFactory,
                               int maxPollRecords,
                               int fetchMaxWaitMs) {
        this.name = name;
        this.topic = topic;
        this.group = group;
        this.type = type;
        this.processor = processor;
        this.sink = sink;
        this.properties = properties;
        this.clientFactory = clientFactory;
        this.maxPollRecords = maxPollRecords;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
    }

    /** 测试用：直接注入已创建的 client，跳过 {@link #initialize()}。 */
    KafkaConsumerRunner(String name,
                        String topic,
                        String group,
                        Class<T> type,
                        KafkaBatchProcessor<T> processor,
                        KafkaSink sink,
                        KafkaClientProperties properties,
                        KafkaClientFactory clientFactory,
                        KafkaConsumer<String, T> consumer,
                        KafkaProducer<String, Object> producer) {
        this(name, topic, group, type, processor, sink, properties, clientFactory,
                properties.consumer().maxPollRecords());
        this.consumer = consumer;
        this.producer = producer;
    }

    @Override
    public void run() {
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                if (!initialize()) {
                    return;
                }
                try {
                    consume();
                } catch (WakeupException e) {
                    if (running.get()) {
                        log.warn("[{}] unexpected consumer wakeup", name, e);
                    }
                } catch (InterruptException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (InvalidConfigurationException e) {
                    log.error("[{}] fatal Kafka configuration error, runner stops", name, e);
                    return;
                } catch (RecordDeserializationException e) {
                    log.error("[{}] record deserialization failed at {}-{}@{}; runner stops",
                            name, e.topicPartition().topic(), e.topicPartition().partition(),
                            e.offset(), e.getCause());
                    return;
                } catch (SerializationException e) {
                    log.error("[{}] Kafka serialization failed; runner stops", name, e);
                    return;
                } catch (ApplicationRecoverableException e) {
                    log.error("[{}] producer must be recreated, recreating clients", name, e);
                } catch (KafkaException e) {
                    log.error("[{}] recoverable Kafka error, recreating clients", name, e);
                } catch (RuntimeException e) {
                    log.error("[{}] unexpected runner failure, recreating clients", name, e);
                } finally {
                    closeClients();
                }
                if (running.get()) {
                    sleepQuietly(properties.retryBackoffMs());
                }
            }
        } finally {
            closeClients();
        }
    }

    /** runner 名称；用于日志、线程命名以及 client.id / transactional.id 的唯一性。 */
    public String name() {
        return name;
    }

    /** 本 runner 使用的 {@code max.poll.records}；批量 topic 小于全局值。 */
    int maxPollRecords() {
        return maxPollRecords;
    }

    /** 请求停止；只唤醒 poll，处理中的批次会自然完成或 abort。 */
    public void stop() {
        running.set(false);
        KafkaConsumer<String, T> currentConsumer = consumer;
        if (currentConsumer != null) {
            try {
                currentConsumer.wakeup();
            } catch (RuntimeException ignored) {
                // 唤醒失败不影响停止流程
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    private boolean initialize() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                KafkaConsumer<String, T> newConsumer = clientFactory.createConsumer(
                        name, group, type, maxPollRecords, fetchMaxWaitMs);
                consumer = newConsumer;
                producer = clientFactory.createProducer("runner-" + name, 0);
                newConsumer.subscribe(List.of(topic), new KafkaRebalanceListener(newConsumer));
                return true;
            } catch (InvalidConfigurationException e) {
                log.error("[{}] fatal Kafka configuration error during init", name, e);
                closeClients();
                return false;
            } catch (InterruptException e) {
                Thread.currentThread().interrupt();
                closeClients();
                return false;
            } catch (RuntimeException e) {
                log.warn("[{}] kafka client init failed, retrying in {}ms: {}",
                        name, properties.retryBackoffMs(), e.toString());
                closeClients();
                sleepQuietly(properties.retryBackoffMs());
            }
        }
        return false;
    }

    private void consume() {
        PolledBatch<T> current = null;
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            if (current == null) {
                current = pollBatch(properties.pollTimeoutMs());
                if (current == null) {
                    continue;
                }
            }
            current = processBatch(current.records());
        }
    }

    /**
     * 一次普通 poll；返回 null 表示本批为空。
     */
    private PolledBatch<T> pollBatch(long timeoutMs) {
        ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(timeoutMs));
        return records.isEmpty() ? null : new PolledBatch<>(records);
    }

    /**
     * 在等 handler 处理当前批之前预取下一批，默认非阻塞（0ms），只推动 fetch 管线。
     *
     * <p>poll 抛非 Kafka 异常时消费者位置不可信，包装成 {@link KafkaException} 走到
     * {@link #run()} 的 client 重建路径，绝不在原地重试整批。
     */
    private PolledBatch<T> pollPrefetch() {
        ConsumerRecords<String, T> records;
        try {
            records = consumer.poll(
                    Duration.ofMillis(properties.consumer().prefetchPollTimeoutMs()));
        } catch (KafkaException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new KafkaException(
                    "prefetch poll failed; consumer position is unknown", e);
        }
        return records.isEmpty() ? null : new PolledBatch<>(records);
    }

    /**
     * 处理一个 poll 批次；可重试错误在方法内部 abort + reset + 退避重试，直到成功或停止。
     * 致命/需要重建 client 的错误向上抛给 {@link #run()}。
     *
     * @return 处理期间预取到的下一批（可能为 null）；成功提交当前批后由 consume 直接使用
     */
    PolledBatch<T> processBatch(ConsumerRecords<String, T> records) {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            boolean transactionOpen = false;
            boolean abortFailed = false;
            PolledBatch<T> prefetched = null;
            try {
                // 在预取 poll 之前捕获本批的 group metadata：若预取期间发生 rebalance，
                // 用旧 generation 提交会失败并整批重投，而不是以新 generation 提交旧分区 offset。
                ConsumerGroupMetadata batchGroupMetadata = consumer.groupMetadata();
                producer.beginTransaction();
                transactionOpen = true;
                sink.bind(producer);
                try {
                    if (processor instanceof PrefetchingBatchProcessor<?>) {
                        @SuppressWarnings("unchecked")
                        PrefetchingBatchProcessor<T> prefetching =
                                (PrefetchingBatchProcessor<T>) processor;
                        prefetching.submit(toList(records));
                        if (running.get() && !Thread.currentThread().isInterrupted()) {
                            prefetched = pollPrefetch();
                        }
                        prefetching.await();
                    } else {
                        processor.process(toList(records));
                    }
                } finally {
                    sink.unbind();
                }
                producer.sendOffsetsToTransaction(offsets(records), batchGroupMetadata);
                producer.commitTransaction();
                transactionOpen = false;
                return prefetched;
            } catch (InvalidConfigurationException | SerializationException e) {
                throw e;
            } catch (ApplicationRecoverableException e) {
                throw e;
            } catch (TransactionAbortableException | CommitFailedException e) {
                log.warn("[{}] retryable transaction failure, retrying batch", name, e);
            } catch (KafkaException e) {
                throw e;
            } catch (RuntimeException e) {
                log.warn("[{}] batch processing failed, retrying whole batch: {}",
                        name, e.toString());
            } finally {
                if (transactionOpen) {
                    abortFailed = !abortQuietly();
                }
            }
            if (abortFailed) {
                throw new IllegalStateException(
                        "[" + name + "] abortTransaction failed; producer will be recreated");
            }
            resetToBatchStart(records);
            resetDiscardedPrefetch(prefetched, records);
            sleepQuietly(properties.retryBackoffMs());
        }
        return null;
    }

    private List<ConsumerRecord<String, T>> toList(ConsumerRecords<String, T> records) {
        List<ConsumerRecord<String, T>> list = new ArrayList<>(records.count());
        for (ConsumerRecord<String, T> record : records) {
            list.add(record);
        }
        return list;
    }

    private Map<TopicPartition, OffsetAndMetadata> offsets(ConsumerRecords<String, T> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, T>> partitionRecords = records.records(partition);
            if (!partitionRecords.isEmpty()) {
                long nextOffset = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
                offsets.put(partition, new OffsetAndMetadata(nextOffset));
            }
        }
        return offsets;
    }

    /**
     * 优先精确 seek 回本批次每个 partition 的第一条 offset；被 rebalance 撤销的分区跳过，
     * seek 失败时退回已提交 offset / beginning。
     */
    private void resetToBatchStart(ConsumerRecords<String, T> records) {
        Set<TopicPartition> assignment;
        try {
            assignment = consumer.assignment();
        } catch (RuntimeException e) {
            log.warn("[{}] cannot read consumer assignment while resetting", name, e);
            return;
        }
        for (TopicPartition partition : records.partitions()) {
            if (!assignment.contains(partition)) {
                log.warn("[{}] partition no longer assigned, skip reset: {}-{}",
                        name, partition.topic(), partition.partition());
                continue;
            }
            List<ConsumerRecord<String, T>> partitionRecords = records.records(partition);
            if (partitionRecords.isEmpty()) {
                continue;
            }
            long firstOffset = partitionRecords.get(0).offset();
            try {
                consumer.seek(partition, firstOffset);
            } catch (RuntimeException e) {
                log.warn("[{}] seek to batch start failed, falling back to committed offset: "
                        + "{}-{}@{}", name, partition.topic(), partition.partition(), firstOffset, e);
                seekCommittedOrBeginning(partition);
            }
        }
    }

    private void seekCommittedOrBeginning(TopicPartition partition) {
        try {
            OffsetAndMetadata committed =
                    consumer.committed(Set.of(partition)).get(partition);
            if (committed != null) {
                consumer.seek(partition, committed.offset());
            } else {
                consumer.seekToBeginning(List.of(partition));
            }
        } catch (RuntimeException e) {
            log.warn("[{}] reset to committed offset failed: {}-{}",
                    name, partition.topic(), partition.partition(), e);
        }
    }

    /**
     * 当前批失败重投时，预取批的 fetch position 已经前移但从未处理；
     * 预取批中不属于当前批的 partition 必须 seek 回已提交位置，否则会丢记录。
     * 与当前批重叠的 partition 由 {@link #resetToBatchStart(ConsumerRecords)} 回到更早的起点。
     *
     * <p>回退失败说明 consumer 位置不可信：直接抛出，走 {@link #run()} 的 client 重建路径。
     * 新 consumer 会从已提交 offset 重新开始，预取但未处理的记录自然重放。
     */
    private void resetDiscardedPrefetch(PolledBatch<T> prefetched,
                                        ConsumerRecords<String, T> current) {
        if (prefetched == null) {
            return;
        }
        Set<TopicPartition> currentPartitions = current.partitions();
        Set<TopicPartition> assignment;
        try {
            assignment = consumer.assignment();
        } catch (RuntimeException e) {
            throw new KafkaException(
                    "cannot read consumer assignment while resetting prefetched batch", e);
        }
        Set<TopicPartition> toReset = new LinkedHashSet<>();
        for (TopicPartition partition : prefetched.records().partitions()) {
            if (currentPartitions.contains(partition)) {
                continue;
            }
            if (!assignment.contains(partition)) {
                log.warn("[{}] prefetched partition no longer assigned, skip reset: {}-{}",
                        name, partition.topic(), partition.partition());
                continue;
            }
            toReset.add(partition);
        }
        if (toReset.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(toReset);
        for (TopicPartition partition : toReset) {
            OffsetAndMetadata offset = committed.get(partition);
            if (offset != null) {
                consumer.seek(partition, offset.offset());
            } else {
                consumer.seekToBeginning(List.of(partition));
            }
        }
    }

    /** @return true 表示 abort 成功，producer 仍可复用 */
    private boolean abortQuietly() {
        try {
            producer.abortTransaction();
            return true;
        } catch (RuntimeException e) {
            log.error("[{}] abortTransaction failed; producer will be recreated", name, e);
            return false;
        }
    }

    private void closeClients() {
        KafkaProducer<String, Object> currentProducer = producer;
        producer = null;
        if (currentProducer != null) {
            try {
                currentProducer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("[{}] producer close failed", name, e);
            }
        }
        KafkaConsumer<String, T> currentConsumer = consumer;
        consumer = null;
        if (currentConsumer != null) {
            try {
                currentConsumer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("[{}] consumer close failed", name, e);
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 一次非空 poll 的结果。 */
    record PolledBatch<T>(ConsumerRecords<String, T> records) {
    }
}
