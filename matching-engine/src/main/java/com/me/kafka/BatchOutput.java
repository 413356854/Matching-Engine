package com.me.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个消费批次的输出缓冲。
 *
 * <p>由 runner 显式创建并作为参数传给 listener → service → publisher，不再使用
 * ThreadLocal 绑定：
 * <ul>
 *   <li>有状态 stage：{@link #capture(String, Map, ConsumerGroupMetadata,
 *       ModuleCommitPipeline)} 收集 {@link ProducerRecord}；service impl 在模块锁内发布完
 *       输出后调用 {@link #enqueue()}，由这里直接构造 {@link SendPackage} 并路由到
 *       {@link ModuleCommitPipeline#enqueue}（内部 {@code queue.put}）。</li>
 *   <li>审计 / 投影 stage：{@link #direct(KafkaProducer)} 立即写入当前 runner 的事务
 *       producer，由 runner 的 {@code commitTransaction()} 统一提交。</li>
 * </ul>
 *
 * <p>一个实例只在一个批次、一个线程内使用，不需要同步。publisher 的代码可以直接看到
 * “记录在这里封装”，service impl 的代码可以直接看到“批次在这里入队”。
 *
 * <p>入队后不等待提交结果；队列长度是唯一的在途窗口，committer 失败由 fatal +
 * 外部重启处理。
 */
public final class BatchOutput {

    /** 批量消息的默认固定 key。 */
    public static final String BATCH_KEY = "batch";

    /** 单条批量消息最多封装的元素数，避免超过 broker 的 1MB 默认上限。 */
    public static final int BATCH_CHUNK_SIZE = 2_000;

    private static final Logger log = LoggerFactory.getLogger(BatchOutput.class);

    private final List<ProducerRecord<String, Object>> records = new ArrayList<>();
    private final KafkaProducer<String, Object> producer;
    private final String source;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;
    private final ConsumerGroupMetadata groupMetadata;
    private final ModuleCommitPipeline pipeline;
    private final boolean discard;
    private boolean enqueued;

    private BatchOutput(KafkaProducer<String, Object> producer,
                        String source,
                        Map<TopicPartition, OffsetAndMetadata> offsets,
                        ConsumerGroupMetadata groupMetadata,
                        ModuleCommitPipeline pipeline,
                        boolean discard) {
        this.producer = producer;
        this.source = source;
        this.offsets = offsets;
        this.groupMetadata = groupMetadata;
        this.pipeline = pipeline;
        this.discard = discard;
    }

    /**
     * 捕获模式：记录先收集到本对象；service impl 处理完后调用 {@link #enqueue()}。
     *
     * <p>传入输入 offset 与 groupMetadata，enqueue 时它们和输出记录一起封装成
     * {@link SendPackage}，由模块 committer 原子提交。
     */
    public static BatchOutput capture(String source,
                                      Map<TopicPartition, OffsetAndMetadata> offsets,
                                      ConsumerGroupMetadata groupMetadata,
                                      ModuleCommitPipeline pipeline) {
        return new BatchOutput(null,
                Objects.requireNonNull(source, "source"),
                Objects.requireNonNull(offsets, "offsets"),
                Objects.requireNonNull(groupMetadata, "groupMetadata"),
                Objects.requireNonNull(pipeline, "pipeline"),
                false);
    }

    /** 直连模式：审计 stage 立即写入 runner 的事务 producer。 */
    public static BatchOutput direct(KafkaProducer<String, Object> producer) {
        return new BatchOutput(Objects.requireNonNull(producer, "producer"),
                null, null, null, null, false);
    }

    /**
     * 丢弃模式：快照恢复重放专用。
     *
     * <p>重放会复用正常 service 的状态机代码，但这些批次在崩溃前已经提交过输出，
     * 不能再发一次；同时 consumer / committer 尚未启动，也不需要入队。
     */
    public static BatchOutput discard(String source) {
        return new BatchOutput(null, Objects.requireNonNull(source, "source"),
                null, null, null, true);
    }

    /**
     * 把本批输出封装成 {@link SendPackage}，直接路由到模块提交队列。
     *
     * <p>必须在 service 的 {@code accountLock} / {@code orderLock} 内调用；内部调用
     * {@link ModuleCommitPipeline#enqueue}，队列满时阻塞在 {@code queue.put}。
     */
    public void enqueue() {
        if (discard) {
            return;
        }
        if (pipeline == null) {
            throw new IllegalStateException(
                    "direct BatchOutput cannot be enqueued; use capture(...)");
        }
        if (enqueued) {
            throw new IllegalStateException("batch already enqueued: " + source);
        }
        SendPackage pkg = new SendPackage(source, records, offsets, groupMetadata);
        try {
            pipeline.enqueue(pkg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while enqueueing batch: " + source, e);
        }
        enqueued = true;
    }

    /** 把一组元素封装成批量消息，使用固定 key {@link #BATCH_KEY}。 */
    public <T> void sendBatchMessage(String topic, List<T> values,
                                     Function<? super List<T>, Object> wrapper) {
        sendBatchMessage(topic, BATCH_KEY, values, wrapper);
    }

    /** 把一组元素按 key 封装成批量消息；超过 {@link #BATCH_CHUNK_SIZE} 时拆成多条。 */
    public <T> void sendBatchMessage(String topic, String key, List<T> values,
                                     Function<? super List<T>, Object> wrapper) {
        if (values == null || values.isEmpty()) {
            return;
        }
        for (int from = 0; from < values.size(); from += BATCH_CHUNK_SIZE) {
            int to = Math.min(from + BATCH_CHUNK_SIZE, values.size());
            emit(new ProducerRecord<>(topic, key, wrapper.apply(values.subList(from, to))));
        }
    }

    /** 显式指定分区；账户 slot 必须与 topic 分区一一对应时使用。 */
    public <T> void sendBatchMessage(String topic, int partition, String key, List<T> values,
                                     Function<? super List<T>, Object> wrapper) {
        if (values == null || values.isEmpty()) {
            return;
        }
        for (int from = 0; from < values.size(); from += BATCH_CHUNK_SIZE) {
            int to = Math.min(from + BATCH_CHUNK_SIZE, values.size());
            emit(new ProducerRecord<>(
                    topic, partition, key, wrapper.apply(values.subList(from, to))));
        }
    }

    /** 产生本批的 runner 名称，用于 fatal 日志。 */
    public String source() {
        return source;
    }

    /** 捕获模式下本批已经封装的记录；直连模式返回空列表。 */
    public List<ProducerRecord<String, Object>> records() {
        return List.copyOf(records);
    }

    private void emit(ProducerRecord<String, Object> record) {
        if (discard) {
            return;
        }
        if (producer == null) {
            records.add(record);
            return;
        }
        producer.send(record, (metadata, error) -> {
            if (error != null) {
                log.error("Kafka send failed asynchronously: topic={}, key={}",
                        record.topic(), record.key(), error);
            }
        });
    }
}
