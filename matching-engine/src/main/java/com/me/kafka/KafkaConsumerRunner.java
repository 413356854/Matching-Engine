package com.me.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一条消费链路的裸 kafka-clients 运行循环。
 *
 * <p>一个 poll 批次一个事务，输出消息与消费 offset 通过
 * {@link KafkaProducer#sendOffsetsToTransaction} 原子提交。任何失败都只 abort 当前事务
 * 并原样抛出，不做整批重试，也不在本进程重建 client：失败批次可能已经修改内存状态，
 * 重试会在脏状态上重复执行；后续由外部监控重启整个进程来丢弃内存状态。
 * 监控接入前，失败只记录 error 日志，进程不会自动重启。
 *
 * <p><b>有状态 stage</b>：service impl 在自己的模块锁内完成业务、分配 seq，
 * 输出写入 runner 显式创建的 {@link BatchOutput}，再连同输入 offset 封装成
 * {@link SendPackage} 放入模块 FIFO 队列；队列满时阻塞入队，不拒绝。
 * 入队后 runner 立即 poll 下一批，<b>不等待</b> committer 提交；committer 单线程按
 * 入队顺序提交，保证提交顺序 == seq 顺序，下一批业务可以和上一批的 Kafka 提交重叠执行。
 *
 * <p><b>审计 / 投影 stage</b>：仍由本 runner 直接开启事务，发送消息并提交 offset
 * （{@code beginTransaction → send → sendOffsetsToTransaction → commitTransaction}）。
 *
 * <p>任何失败都只 abort 当前事务并进入全局 fatal，不做整批重试、不在本进程重建 client：
 * 失败批次可能已经修改内存状态，重试会在脏状态上重复执行；后续由外部监控重启整个进程。
 */
public final class KafkaConsumerRunner<T> implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerRunner.class);

    private final String name;
    private final String topic;
    private final String group;
    private final Class<T> type;
    private final KafkaBatchProcessor<T> processor;
    private final KafkaClientProperties properties;
    private final KafkaClientFactory clientFactory;
    private final FatalState fatalState;
    /**
     * 有状态 stage 的模块提交管线；审计 stage 为 null。
     *
     * <p>管线持有 FIFO 提交队列；service 在模块锁内完成业务并入队，runner 不等待提交结果。
     */
    private final ModuleCommitPipeline commitPipeline;
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
                               KafkaClientProperties properties,
                               KafkaClientFactory clientFactory,
                               FatalState fatalState,
                               ModuleCommitPipeline commitPipeline) {
        this(name, topic, group, type, processor, properties, clientFactory, fatalState,
                commitPipeline, properties.consumer().maxPollRecords(),
                properties.consumer().fetchMaxWaitMs());
    }

    public KafkaConsumerRunner(String name,
                               String topic,
                               String group,
                               Class<T> type,
                               KafkaBatchProcessor<T> processor,
                               KafkaClientProperties properties,
                               KafkaClientFactory clientFactory,
                               FatalState fatalState,
                               ModuleCommitPipeline commitPipeline,
                               int maxPollRecords) {
        this(name, topic, group, type, processor, properties, clientFactory, fatalState,
                commitPipeline, maxPollRecords, properties.consumer().fetchMaxWaitMs());
    }

    public KafkaConsumerRunner(String name,
                               String topic,
                               String group,
                               Class<T> type,
                               KafkaBatchProcessor<T> processor,
                               KafkaClientProperties properties,
                               KafkaClientFactory clientFactory,
                               FatalState fatalState,
                               ModuleCommitPipeline commitPipeline,
                               int maxPollRecords,
                               int fetchMaxWaitMs) {
        this.name = name;
        this.topic = topic;
        this.group = group;
        this.type = type;
        this.processor = processor;
        this.properties = properties;
        this.clientFactory = clientFactory;
        this.fatalState = fatalState;
        this.commitPipeline = commitPipeline;
        this.maxPollRecords = maxPollRecords;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
    }

    /** 测试用：直接注入已创建的 client，跳过 {@link #initialize()}。 */
    KafkaConsumerRunner(String name,
                        String topic,
                        String group,
                        Class<T> type,
                        KafkaBatchProcessor<T> processor,
                        KafkaClientProperties properties,
                        KafkaClientFactory clientFactory,
                        FatalState fatalState,
                        ModuleCommitPipeline commitPipeline,
                        KafkaConsumer<String, T> consumer,
                        KafkaProducer<String, Object> producer) {
        this(name, topic, group, type, processor, properties, clientFactory, fatalState,
                commitPipeline, properties.consumer().maxPollRecords());
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
                consume();
            }
        } catch (WakeupException e) {
            if (running.get()) {
                fatalState.markFatal(name, e);
                throw e;
            }
        } catch (InterruptException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            /*
             * 不在本进程重试或重建 client：失败批次可能已经修改了内存状态，
             * 重试会在脏状态上重复执行。这里进入全局 fatal：停止全部 consumer，
             * HTTP 入口返回 503，进程等待外部监控重启，用重启丢弃内存状态。
             * 监控接入前，fatal 只体现在日志和 503 上，进程不会自动退出。
             */
            fatalState.markFatal(name, e);
            throw e;
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
        try {
            KafkaConsumer<String, T> newConsumer = clientFactory.createConsumer(
                    name, group, type, maxPollRecords, fetchMaxWaitMs);
            consumer = newConsumer;
            if (commitPipeline == null) {
                // 有状态 stage 的输出由模块 committer 的事务型 producer 统一提交。
                producer = clientFactory.createProducer("runner-" + name, 0);
            }
            newConsumer.subscribe(List.of(topic), new KafkaRebalanceListener(newConsumer));
            return true;
        } catch (WakeupException e) {
            if (running.get()) {
                fatalState.markFatal(name, e);
                throw e;
            }
            return false;
        } catch (InterruptException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (RuntimeException e) {
            // 同 processBatch：不在本进程重试，进入全局 fatal，等待外部监控重启进程。
            fatalState.markFatal(name, e);
            throw e;
        }
    }

    private void consume() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            PolledBatch<T> current = pollBatch(properties.pollTimeoutMs());
            if (current != null) {
                processBatch(current.records());
            }
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
     * 处理一个 poll 批次；只执行一次，不做整批重试。
     *
     * <p>有状态 stage 走 {@link #processStatefulBatch}：service impl 在自己的
     * accountLock / orderLock 内处理、发布并 {@code output.enqueue()} 入队，runner
     * 入队后立即 poll 下一批；审计 / 投影 stage 走 {@link #processInTransaction}，
     * 沿用单 runner 事务。
     */
    void processBatch(ConsumerRecords<String, T> records) {
        if (fatalState.isFatal()) {
            throw new IllegalStateException("[" + name + "] engine is fatal; refusing new batch");
        }
        if (commitPipeline != null) {
            processStatefulBatch(records);
            return;
        }
        processInTransaction(records);
    }

    /**
     * 有状态 stage：service impl 在自己的 accountLock / orderLock 内完成业务、分配 seq、
     * 发布输出并调用 {@code output.enqueue()}；runner 不持有模块锁，也不等待提交结果，
     * 入队后立即回去 poll 下一批。
     *
     * <p>队列顺序由 service 的批次锁保证；在途窗口只由队列长度控制；队列满时 service
     * 在锁内阻塞在 {@code queue.put}，形成背压。commit 失败进入 fatal，由外部重启处理。
     */
    private void processStatefulBatch(ConsumerRecords<String, T> records) {
        if (!running.get() || Thread.currentThread().isInterrupted()) {
            return;
        }
        if (fatalState.isFatal()) {
            throw new IllegalStateException(
                    "[" + name + "] engine is fatal; refusing batch");
        }
        ConsumerGroupMetadata batchGroupMetadata = consumer.groupMetadata();

        // 输出缓冲由 runner 显式创建，沿 listener → service → publisher 传递；
        // service 在锁内发布完输出后调用 output.enqueue()，直接路由到 queue.put。
        BatchOutput output = BatchOutput.capture(
                name, offsets(records), batchGroupMetadata, commitPipeline);
        try {
            processor.process(toList(records), output);
        } catch (RuntimeException e) {
            if (!running.get()) {
                // 正常停机期间 committer 已停止；offset 未提交，重启后重新消费。
                return;
            }
            // service 已在持锁状态下标记 fatal；这里兜底（幂等）。
            fatalState.markFatal(name, e);
            throw e;
        }
    }

    /**
     * 审计 / 投影 stage：单 runner 直接开启本地事务，处理完提交输入 offset。
     * 输出缓冲使用直连模式，记录会立即写入当前事务 producer。
     */
    private void processInTransaction(ConsumerRecords<String, T> records) {
        boolean transactionOpen = false;
        try {
            ConsumerGroupMetadata batchGroupMetadata = consumer.groupMetadata();
            producer.beginTransaction();
            transactionOpen = true;

            BatchOutput output = BatchOutput.direct(producer);
            processor.process(toList(records), output);

            producer.sendOffsetsToTransaction(offsets(records), batchGroupMetadata);

            if (fatalState.isFatal()) {
                throw new IllegalStateException(
                        "[" + name + "] engine is fatal; aborting batch before commit");
            }
            producer.commitTransaction();
            transactionOpen = false;
        } catch (RuntimeException e) {
            // 失败批次可能已改内存：先进入全局 fatal（停止全部 consumer），再 abort/抛出。
            fatalState.markFatal(name, e);
            throw e;
        } finally {
            if (transactionOpen) {
                abortQuietly();
            }
        }
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

    /** 尽力 abort 当前事务；失败只记录日志，原始异常继续向上抛。 */
    private void abortQuietly() {
        try {
            producer.abortTransaction();
        } catch (RuntimeException e) {
            log.error("[{}] abortTransaction failed; process restart required", name, e);
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

    /** 一次非空 poll 的结果。 */
    record PolledBatch<T>(ConsumerRecords<String, T> records) {
    }
}
