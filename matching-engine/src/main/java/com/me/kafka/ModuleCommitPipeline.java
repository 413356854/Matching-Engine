package com.me.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * 单个模块（委托 / 账户）的 FIFO 事务提交管线。
 *
 * <p>有状态 runner 在模块锁内完成业务、分配 seq，并把输出消息与输入 offset 收集成
 * {@link SendPackage} 入队；本类唯一的 committer 线程按入队顺序开启 Kafka 事务，
 * 一次最多合并 {@code maxPackagesPerTransaction} 个包，统一发送消息、提交消费 offset、
 * commit。runner 入队后立即回去 poll 下一批，不等待提交结果。
 *
 * <p><b>FIFO</b>：入队由 service 在 accountLock / orderLock 内通过
 * {@link BatchOutput#enqueue()} 触发，因此队列顺序 == seq 分配顺序 == 状态推进顺序；
 * committer 单线程取包，提交顺序与入队顺序一致。
 *
 * <p><b>背压</b>：队列有界（默认 32），满时 {@code put()} 阻塞持有模块锁的 runner，
 * 不拒绝、不超时；压力沿 consumer lag → 下单 ingress ring → HTTP 503 向上传导。
 *
 * <p><b>失败语义</b>：committer 任一事务失败即 abort、进入全局 fatal、丢弃本批和
 * 队列中所有未提交包，并停止 committer；后续 runner / service 看到 fatal 退出，
 * 由外部监控重启整个进程。
 *
 * <p><b>flush</b>：快照需要确认「已入队的包全部提交完成」。{@link #flush(long)} 按包子数
 * 等待 {@code committedPackages >= enqueuedPackages(target)}；committer 失败或停止时
 * 立即抛错，不会让快照在未提交状态上继续。
 */
public final class ModuleCommitPipeline implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ModuleCommitPipeline.class);

    /** 统计日志周期；只由 committer 线程读写，不需要同步。 */
    private static final long STATS_LOG_INTERVAL_NANOS = 5_000_000_000L;

    private final String name;
    private final KafkaClientFactory clientFactory;
    private final FatalState fatalState;
    private final BlockingQueue<SendPackage> queue;
    private final int queueCapacity;
    private final int maxPackagesPerTransaction;
    private final AtomicBoolean running = new AtomicBoolean();
    private final CommitStats stats = new CommitStats();

    /**
     * flush 等待条件：入队 / 提交的包子数，以及 committer 是否已经停止。
     *
     * <p>包在 {@link #enqueue} 里、真正放入队列之前先计数；提交成功后在 committer
     * 线程里累加已提交数。这样 {@link #flush(long)} 不会把「已经进入队列但还没被
     * committer 看到」的包漏掉。
     */
    private final Object flushMonitor = new Object();
    private long enqueuedPackages;
    private long committedPackages;
    private boolean committerStalled;

    private volatile KafkaProducer<String, Object> producer;
    private volatile Thread committerThread;
    private long lastStatsLogNanos = System.nanoTime();

    public ModuleCommitPipeline(String name,
                                KafkaClientFactory clientFactory,
                                FatalState fatalState,
                                int queueCapacity,
                                int maxPackagesPerTransaction) {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive: " + queueCapacity);
        }
        if (maxPackagesPerTransaction <= 0) {
            throw new IllegalArgumentException(
                    "max packages per transaction must be positive: " + maxPackagesPerTransaction);
        }
        this.name = name;
        this.clientFactory = clientFactory;
        this.fatalState = fatalState;
        this.queue = new ArrayBlockingQueue<>(queueCapacity, true);
        this.queueCapacity = queueCapacity;
        this.maxPackagesPerTransaction = maxPackagesPerTransaction;
    }

    /**
     * 入队；队列满时阻塞，不拒绝、不超时。
     *
     * <p>由 {@link BatchOutput#enqueue()} 在 service 的 accountLock / orderLock 内调用：
     * 队列顺序就是 seq 顺序和提交顺序。committer 已停止或引擎 fatal 时直接抛错，
     * 避免把包放进一个不会再被消费的队列。
     */
    public void enqueue(SendPackage pkg) throws InterruptedException {
        if (!running.get() || fatalState.isFatal()) {
            throw new IllegalStateException(
                    "[" + name + "] transaction committer is not running");
        }
        // 先计数再 put：flush 的目标包数必须覆盖已经决定入队的包，
        // 否则 flush 可能在 put 完成前看到 queue 为空而提前返回。
        synchronized (flushMonitor) {
            enqueuedPackages++;
        }
        long start = System.nanoTime();
        try {
            queue.put(pkg);
        } catch (InterruptedException e) {
            synchronized (flushMonitor) {
                enqueuedPackages--;
                // 该包没有入队；调用方会进入 fatal，flush 不应继续等待一个不存在的包。
                committerStalled = true;
                flushMonitor.notifyAll();
            }
            throw e;
        }
        stats.recordEnqueue(System.nanoTime() - start, queue.size());
    }

    /**
     * 等待调用时已经入队的包全部提交完成。
     *
     * <p>只等待 {@code target = 调用瞬间的 enqueuedPackages}；调用之后新入队的包不在
     * 本次等待范围内。快照在持有模块锁、不会再有新包入队时调用，因此最终一次 flush
     * 就是完整的静止点。
     *
     * @throws IllegalStateException committer 已停止 / fatal / 超时 / 中断
     */
    public void flush(long timeoutMillis) {
        long target;
        synchronized (flushMonitor) {
            target = enqueuedPackages;
        }
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(Math.max(1L, timeoutMillis));
        synchronized (flushMonitor) {
            while (committedPackages < target) {
                if (committerStalled) {
                    throw new IllegalStateException(
                            "[" + name + "] committer stopped before flush completed");
                }
                if (fatalState.isFatal()) {
                    throw new IllegalStateException(
                            "[" + name + "] engine is fatal; flush aborted");
                }
                if (!running.get()) {
                    throw new IllegalStateException(
                            "[" + name + "] committer is not running; flush aborted");
                }
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new IllegalStateException("[" + name + "] flush timed out: enqueued="
                            + enqueuedPackages + ", committed=" + committedPackages);
                }
                try {
                    flushMonitor.wait(Math.max(1L,
                            TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("[" + name + "] flush interrupted", e);
                }
            }
        }
    }

    /** 已入队但尚未提交完成的包子数；用于快照等待日志。 */
    public long inFlightPackages() {
        synchronized (flushMonitor) {
            return enqueuedPackages - committedPackages;
        }
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        synchronized (flushMonitor) {
            enqueuedPackages = 0;
            committedPackages = 0;
            committerStalled = false;
        }
        KafkaProducer<String, Object> txProducer =
                clientFactory.createProducer(name, 0);
        producer = txProducer;
        Thread thread = new Thread(this::commitLoop, name + "-thread");
        thread.setDaemon(true);
        committerThread = thread;
        thread.start();
        log.info("[{}] transaction committer started, queueCapacity={}, maxPackagesPerTransaction={}",
                name, queueCapacity, maxPackagesPerTransaction);
    }

    private void commitLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            SendPackage first;
            try {
                first = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            List<SendPackage> batch = new ArrayList<>(maxPackagesPerTransaction);
            batch.add(first);
            if (maxPackagesPerTransaction > 1) {
                // 不等待第二个包：只有已经排队的包才合并进同一个事务。
                queue.drainTo(batch, maxPackagesPerTransaction - 1);
            }
            commit(batch);
            maybeLogStats();
        }
        clearPending();
        logStats();
    }

    private void maybeLogStats() {
        long now = System.nanoTime();
        if (now - lastStatsLogNanos < STATS_LOG_INTERVAL_NANOS) {
            return;
        }
        lastStatsLogNanos = now;
        logStats();
    }

    private void logStats() {
        long transactions = stats.transactions.sum();
        if (transactions == 0) {
            return;
        }
        long enqueues = stats.enqueueCount.sum();
        double perTxn = transactions;
        log.info(String.format(Locale.ROOT,
                "[%s] stats txns=%d failed=%d pkgs/txn=%.2f offsetCalls/txn=%.2f records/txn=%.1f "
                        + "begin=%.3fms sendEnq=%.3fms offsets=%.3fms commit=%.3fms total=%.3fms "
                        + "maxSend=%.2fms maxOffsets=%.2fms maxCommit=%.2fms maxTotal=%.2fms "
                        + "enqWait=%.3fms enqCount=%d maxQueue=%d",
                name,
                transactions,
                stats.failedTransactions.sum(),
                stats.packages.sum() / perTxn,
                stats.offsetCalls.sum() / perTxn,
                stats.records.sum() / perTxn,
                stats.beginNanos.sum() / 1e6 / perTxn,
                stats.sendNanos.sum() / 1e6 / perTxn,
                stats.offsetsNanos.sum() / 1e6 / perTxn,
                stats.commitNanos.sum() / 1e6 / perTxn,
                stats.totalNanos.sum() / 1e6 / perTxn,
                stats.maxSendNanos.get() / 1e6,
                stats.maxOffsetsNanos.get() / 1e6,
                stats.maxCommitNanos.get() / 1e6,
                stats.maxTotalNanos.get() / 1e6,
                enqueues == 0 ? 0.0 : stats.enqueueWaitNanos.sum() / 1e6 / enqueues,
                enqueues,
                stats.maxQueueDepth.get()));
    }

    /** committer / runner 共用的累计统计；高频计数用 LongAdder。 */
    private static final class CommitStats {

        private final LongAdder transactions = new LongAdder();
        private final LongAdder failedTransactions = new LongAdder();
        private final LongAdder packages = new LongAdder();
        private final LongAdder offsetCalls = new LongAdder();
        private final LongAdder records = new LongAdder();
        private final LongAdder beginNanos = new LongAdder();
        private final LongAdder sendNanos = new LongAdder();
        private final LongAdder offsetsNanos = new LongAdder();
        private final LongAdder commitNanos = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final LongAdder enqueueCount = new LongAdder();
        private final LongAdder enqueueWaitNanos = new LongAdder();
        private final AtomicLong maxSendNanos = new AtomicLong();
        private final AtomicLong maxOffsetsNanos = new AtomicLong();
        private final AtomicLong maxCommitNanos = new AtomicLong();
        private final AtomicLong maxTotalNanos = new AtomicLong();
        private final AtomicLong maxQueueDepth = new AtomicLong();

        private void record(int packageCount, int offsetCallCount, long recordCount,
                            long begin, long send, long offsets, long commit, long total) {
            transactions.increment();
            packages.add(packageCount);
            offsetCalls.add(offsetCallCount);
            records.add(recordCount);
            beginNanos.add(begin);
            sendNanos.add(send);
            offsetsNanos.add(offsets);
            commitNanos.add(commit);
            totalNanos.add(total);
            maxSendNanos.accumulateAndGet(send, Math::max);
            maxOffsetsNanos.accumulateAndGet(offsets, Math::max);
            maxCommitNanos.accumulateAndGet(commit, Math::max);
            maxTotalNanos.accumulateAndGet(total, Math::max);
        }

        private void recordEnqueue(long waitNanos, int queueDepth) {
            enqueueCount.increment();
            enqueueWaitNanos.add(waitNanos);
            maxQueueDepth.accumulateAndGet(queueDepth, Math::max);
        }
    }

    private void commit(List<SendPackage> batch) {
        boolean transactionOpen = false;
        long transactionStart = System.nanoTime();
        try {
            long beginStart = System.nanoTime();
            producer.beginTransaction();
            long beginNanos = System.nanoTime() - beginStart;
            transactionOpen = true;

            long recordCount = 0;
            long sendStart = System.nanoTime();
            for (SendPackage pkg : batch) {
                for (ProducerRecord<String, Object> record : pkg.records()) {
                    producer.send(record, (metadata, error) -> {
                        if (error != null) {
                            log.error("[{}] kafka send failed: topic={}, key={}, source={}",
                                    name, record.topic(), record.key(), pkg.source(), error);
                        }
                    });
                    recordCount++;
                }
            }
            // send 是异步入队，这里只统计入队耗时；真实网络发送在 commit 内等待完成。
            long sendNanos = System.nanoTime() - sendStart;

            long offsetsStart = System.nanoTime();
            // 先按 ConsumerGroupMetadata 分组，再在组内按 TopicPartition 合并；
            // 同一 consumer 的多个在途包只提交一次 offset。不同 member / generation
            // 的包不能混用 metadata，必须分别提交。
            Map<ConsumerGroupMetadata, Map<TopicPartition, OffsetAndMetadata>> offsetsByGroup =
                    new LinkedHashMap<>();
            for (SendPackage pkg : batch) {
                if (pkg.offsets().isEmpty()) {
                    continue;
                }
                Map<TopicPartition, OffsetAndMetadata> merged =
                        offsetsByGroup.computeIfAbsent(
                                pkg.groupMetadata(), ignored -> new HashMap<>());
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry
                        : pkg.offsets().entrySet()) {
                    merged.merge(entry.getKey(), entry.getValue(),
                            (left, right) -> left.offset() >= right.offset() ? left : right);
                }
            }
            int offsetCalls = 0;
            for (Map.Entry<ConsumerGroupMetadata, Map<TopicPartition, OffsetAndMetadata>> entry
                    : offsetsByGroup.entrySet()) {
                producer.sendOffsetsToTransaction(entry.getValue(), entry.getKey());
                offsetCalls++;
            }
            long offsetsNanos = System.nanoTime() - offsetsStart;

            long commitStart = System.nanoTime();
            producer.commitTransaction();
            long commitNanos = System.nanoTime() - commitStart;
            long totalNanos = System.nanoTime() - transactionStart;
            transactionOpen = false;

            stats.record(batch.size(), offsetCalls, recordCount, beginNanos, sendNanos,
                    offsetsNanos, commitNanos, totalNanos);
            synchronized (flushMonitor) {
                committedPackages += batch.size();
                flushMonitor.notifyAll();
            }
        } catch (RuntimeException e) {
            stats.failedTransactions.increment();
            if (transactionOpen) {
                try {
                    producer.abortTransaction();
                } catch (RuntimeException abortError) {
                    log.error("[{}] abortTransaction failed after commit error", name, abortError);
                }
            }
            fatalState.markFatal(name, e);
            clearPending();
            running.set(false);
        }
    }

    /** 丢弃队列中未提交的包；runner 不等待提交结果，失败由 fatal + 重启处理。 */
    private void clearPending() {
        List<SendPackage> pending = new ArrayList<>();
        queue.drainTo(pending);
        if (!pending.isEmpty()) {
            log.warn("[{}] dropped {} queued package(s) after committer stop", name, pending.size());
        }
        // 无论正常停机还是失败，都唤醒 flush：未提交的包不会再被提交。
        synchronized (flushMonitor) {
            committerStalled = true;
            flushMonitor.notifyAll();
        }
    }

    @Override
    public void stop() {
        boolean wasRunning = running.getAndSet(false);
        Thread thread = committerThread;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(Duration.ofSeconds(10).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        clearPending();
        KafkaProducer<String, Object> txProducer = producer;
        producer = null;
        if (txProducer != null) {
            try {
                txProducer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("[{}] producer close failed", name, e);
            }
        }
        if (wasRunning) {
            log.info("[{}] transaction committer stopped", name);
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        // 早于 KafkaConsumerManager(100) 启动、晚于它停止：先停消费者，再停 committer。
        return 50;
    }
}
