package com.me.snapshot;

import com.me.account.service.AccountBatchLock;
import com.me.account.service.AccountEntryLock;
import com.me.account.service.AccountOperationService;
import com.me.account.service.AccountService;
import com.me.account.vo.AccountView;
import com.me.kafka.FatalState;
import com.me.kafka.KafkaClientProperties;
import com.me.kafka.ModuleCommitPipeline;
import com.me.matching.service.MatchingEngine;
import com.me.matching.vo.OrderBookSnapshot;
import com.me.order.dto.OrderStateSnapshot;
import com.me.order.service.OrderBatchLock;
import com.me.order.service.OrderEntryLocks;
import com.me.order.service.OrderOperationService;
import com.me.order.service.OrderService;
import com.me.snapshot.dto.ReplayCheckpoint;
import com.me.snapshot.dto.SnapshotBackupDetail;
import com.me.snapshot.dto.SnapshotBackupRecord;
import com.me.snapshot.dto.SnapshotFile;
import com.me.snapshot.dto.SnapshotPayload;
import com.me.snapshot.dto.SnapshotResult;
import com.me.snapshot.dto.TopicPartitionOffset;
import com.me.snapshot.entity.SnapshotBackup;
import com.me.snapshot.entity.SnapshotBackupOffset;
import com.me.snapshot.recovery.RecoveryState;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * 备份快照：入口锁 + committer flush + 内部 topic 排空 + 模块锁内复制 + 原子写文件。
 *
 * <p><b>为什么需要入口锁</b>：快照要的是一个「不再有新外部输入、内部级联已排空」的
 * 静止点。下单 / 撤单 / 充值三个入口 consumer 在进入模块批次锁之前先拿入口锁；
 * 快照按固定顺序拿全三把入口锁后，新入口批次不会再驱动状态机。冻结、结算等内部
 * 链路不拿入口锁，因此可以继续排空。
 *
 * <p><b>为什么不能只看入口锁</b>：committer 是异步 FIFO 队列，入口批次释放模块锁时
 * 输出包可能还在队列里。快照必须调用 {@link ModuleCommitPipeline#flush(long)} 等到
 * 已入队的包全部提交，否则会把「内存已改、Kafka 未提交」的状态写进快照。
 *
 * <p><b>排空判据</b>：三个内部 topic
 * ({@code me.account.order-freeze-commands-batch}、{@code me.order.freeze-results-batch}、
 * {@code me.account.settlements-batch}) 的所有分区都满足
 * {@code 消费组 committed offset >= read_committed end offset}。内部 topic 会互相
 * 级联产出，所以必须循环检查到同时满足；最后在账户 / 委托模块锁内再 flush + 复检
 * 一次，得到真正静止的切点。
 *
 * <p><b>事务 marker</b>：内部 topic 由事务型 producer 写入，数据记录后面跟着 commit
 * marker。read_committed 消费者会跳过 marker，所以 committed offset 可能比 LSO 少 1。
 * 排空检查在 {@code committed < end} 时用探针 consumer 从 committed 位置读一次，确认
 * gap 里只有 marker / aborted 记录、没有未消费的数据记录，才判定排空。
 *
 * <p><b>复制</b>：在 accountLock、orderLock（以及引擎对象锁）内复制账户、委托、订单簿
 * 和 seq 水位。复制结果全部是不可变副本，释放锁后再做校验、序列化和落盘，避免长时间
 * 阻塞消费。
 *
 * <p><b>恢复不在本阶段实现</b>：快照文件已经记录六条状态机输入 topic 的 next offset、
 * 两个 offset-seq topic 在切点的端位点、accountSeq / orderSeq 水位、
 * tradeSettlementIdGenerator 和各订单簿 nextTradeId，供后续加载 / 重放使用。
 *
 * <p><b>单实例前提</b>：入口锁、模块锁、账户 / 委托 / 订单簿状态都是 JVM 内的；只有
 * 单个实例拥有全部输入分区时，快照才是全局一致的状态。多实例部署需要另行设计分区
 * 级快照或共享状态。
 */
@Service
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class SnapshotService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);
    private static final long RETRY_DELAY_MS = 200L;

    private final KafkaClientProperties kafkaProperties;
    private final AdminClient adminClient;
    private final ModuleCommitPipeline orderCommitPipeline;
    private final ModuleCommitPipeline accountCommitPipeline;
    private final OrderEntryLocks orderEntryLocks;
    private final AccountEntryLock accountEntryLock;
    private final AccountBatchLock accountBatchLock;
    private final OrderBatchLock orderBatchLock;
    private final AccountService accountService;
    private final OrderService orderService;
    private final MatchingEngine matchingEngine;
    private final AccountOperationService accountOperationService;
    private final OrderOperationService orderOperationService;
    private final FatalState fatalState;
    private final SnapshotProperties properties;
    private final SnapshotWriter writer;
    private final SnapshotBackupStore backupStore;
    private final RecoveryState recoveryState;

    /** 同一进程内只允许一个快照；不同快照共享静态状态，并发执行没有意义。 */
    private final ReentrantLock snapshotLock = new ReentrantLock(true);

    /** 内部级联 topic；快照必须等它们全部分区排空。 */
    private final List<Target> internalTargets;

    /** 状态机全部输入 topic；快照为每个分区记录恢复起点。 */
    private final List<Target> inputTargets;

    /**
     * offset-seq topic；快照记录它们在切点的逐分区端位点，恢复时从这里开始读映射，
     * 不必从 earliest 全量扫描。
     */
    private final List<String> replayCheckpointTopics;

    /** topic → 分区号列表；topic 分区数在运行期不变，首次查询后缓存。 */
    private final Map<String, List<Integer>> partitionCache = new ConcurrentHashMap<>();

    /**
     * 排空探针：只在 {@code committed < end} 时使用，从 committed 位置读一次，
     * 区分「只剩事务 marker」和「还有未消费的数据记录」。懒创建、单快照线程使用。
     */
    private volatile KafkaConsumer<byte[], byte[]> drainProbe;

    public SnapshotService(
            KafkaClientProperties kafkaProperties,
            AdminClient adminClient,
            @Qualifier("orderCommitPipeline") ModuleCommitPipeline orderCommitPipeline,
            @Qualifier("accountCommitPipeline") ModuleCommitPipeline accountCommitPipeline,
            OrderEntryLocks orderEntryLocks,
            AccountEntryLock accountEntryLock,
            AccountBatchLock accountBatchLock,
            OrderBatchLock orderBatchLock,
            AccountService accountService,
            OrderService orderService,
            MatchingEngine matchingEngine,
            AccountOperationService accountOperationService,
            OrderOperationService orderOperationService,
            FatalState fatalState,
            SnapshotProperties properties,
            SnapshotWriter writer,
            SnapshotBackupStore backupStore,
            RecoveryState recoveryState) {
        this.kafkaProperties = kafkaProperties;
        this.adminClient = adminClient;
        this.orderCommitPipeline = orderCommitPipeline;
        this.accountCommitPipeline = accountCommitPipeline;
        this.orderEntryLocks = orderEntryLocks;
        this.accountEntryLock = accountEntryLock;
        this.accountBatchLock = accountBatchLock;
        this.orderBatchLock = orderBatchLock;
        this.accountService = accountService;
        this.orderService = orderService;
        this.matchingEngine = matchingEngine;
        this.accountOperationService = accountOperationService;
        this.orderOperationService = orderOperationService;
        this.fatalState = fatalState;
        this.properties = properties;
        this.writer = writer;
        this.backupStore = backupStore;
        this.recoveryState = recoveryState;

        this.internalTargets = List.of(
                target("account-order-freeze-commands", "account-order-freeze"),
                target("order-freeze-results", "order-freeze-result"),
                target("account-settlements", "account-settlement"));
        this.inputTargets = List.of(
                target("order-commands", "order"),
                target("order-cancels", "order-cancel"),
                target("order-freeze-results", "order-freeze-result"),
                target("account-commands", "account"),
                target("account-order-freeze-commands", "account-order-freeze"),
                target("account-settlements", "account-settlement"));
        this.replayCheckpointTopics = List.of(
                topic("account-offset-seq"),
                topic("order-offset-seq"));
    }

    /**
     * 执行一次快照。同步阻塞到文件写完；调用方通常是 admin HTTP 端点。
     *
     * @throws SnapshotInProgressException 已有快照在执行
     * @throws SnapshotUnavailableException fatal / committer 停止 / 超时 / 一致性校验失败
     */
    public SnapshotResult createSnapshot() {
        if (!snapshotLock.tryLock()) {
            throw new SnapshotInProgressException("snapshot is already running");
        }
        try {
            if (!recoveryState.isReady()) {
                throw new SnapshotUnavailableException(
                        "engine is recovering from a snapshot; refusing to take a new snapshot");
            }
            if (fatalState.isFatal()) {
                throw new SnapshotUnavailableException(
                        "engine is fatal; refusing to take a snapshot");
            }
            if (!orderCommitPipeline.isRunning() || !accountCommitPipeline.isRunning()) {
                throw new SnapshotUnavailableException(
                        "transaction committer is not running; refusing to take a snapshot");
            }

            long startNanos = System.nanoTime();
            List<ReentrantLock> entryLocks = List.of(
                    orderEntryLocks.placementLock(),
                    orderEntryLocks.cancelLock(),
                    accountEntryLock.reentrantLock());
            acquireEntryLocks(entryLocks);

            SnapshotPayload payload;
            try {
                payload = awaitQuiescenceAndCapture();
            } finally {
                releaseLocks(entryLocks);
            }
            long captureNanos = System.nanoTime();

            if (properties.validateInvariant()) {
                validateInvariant(payload);
            }

            Instant createdAt = Instant.parse(payload.createdAt());
            SnapshotWriter.SnapshotWriteResult written = writer.write(payload, createdAt);
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startNanos);
            long captureMillis = TimeUnit.NANOSECONDS.toMillis(captureNanos - startNanos);
            long backupId = recordBackup(payload, written, durationMillis);

            log.info("snapshot completed: backupId={}, path={}, accounts={}, orders={}, "
                            + "books={}, offsets={}, replayCheckpoints={}, accountSeq={}, "
                            + "orderSeq={}, capture={}ms, total={}ms, checksum={}",
                    backupId, written.path(), payload.accounts().size(),
                    payload.orderState().orders().size(), payload.books().size(),
                    payload.inputOffsets().size(), payload.replayCheckpoints().size(),
                    payload.accountSeq(), payload.orderSeq(),
                    captureMillis, durationMillis, written.checksum());

            return new SnapshotResult(
                    backupId,
                    written.path().toString(),
                    written.sizeBytes(),
                    payload.createdAt(),
                    payload.accountSeq(),
                    payload.orderSeq(),
                    payload.accounts().size(),
                    payload.orderState().orders().size(),
                    payload.books().size(),
                    payload.inputOffsets().size(),
                    durationMillis,
                    written.checksum());
        } finally {
            snapshotLock.unlock();
        }
    }

    /**
     * 把本次快照的恢复水位、文件路径和统计信息写入 MySQL。
     *
     * <p>文件已经原子落盘；数据库写入失败时不会删除文件，而是抛出带路径的错误，
     * 让管理端知道“文件存在但未登记”。主表 + offset 表在同一个事务里写入。
     */
    private long recordBackup(SnapshotPayload payload,
                              SnapshotWriter.SnapshotWriteResult written,
                              long durationMillis) {
        SnapshotBackup backup = new SnapshotBackup();
        backup.setFormatVersion(SnapshotFile.CURRENT_FORMAT_VERSION);
        backup.setInstanceId(payload.instanceId());
        backup.setFilePath(written.path().toString());
        backup.setFileSizeBytes(written.sizeBytes());
        backup.setChecksum(written.checksum());
        backup.setSnapshotCreatedAt(LocalDateTime.ofInstant(
                Instant.parse(payload.createdAt()), ZoneOffset.UTC));
        backup.setAccountSeq(payload.accountSeq());
        backup.setOrderSeq(payload.orderSeq());
        backup.setAccountCount(payload.accounts().size());
        backup.setOrderCount(payload.orderState().orders().size());
        backup.setBookCount(payload.books().size());
        backup.setOffsetCount(payload.inputOffsets().size());
        backup.setDurationMs(durationMillis);

        List<SnapshotBackupOffset> offsets = new ArrayList<>(payload.inputOffsets().size());
        for (TopicPartitionOffset offset : payload.inputOffsets()) {
            SnapshotBackupOffset entity = new SnapshotBackupOffset();
            entity.setTopic(offset.topic());
            entity.setConsumerGroup(offset.group());
            entity.setPartitionNo(offset.partition());
            entity.setNextOffset(offset.nextOffset());
            offsets.add(entity);
        }

        try {
            return backupStore.save(backup, offsets);
        } catch (RuntimeException e) {
            log.error("snapshot file written but failed to record backup metadata: "
                    + "path={}, checksum={}", written.path(), written.checksum(), e);
            throw new SnapshotUnavailableException(
                    "snapshot file written to " + written.path()
                            + " but failed to record backup metadata: " + e.getMessage(), e);
        }
    }

    /** 最近快照列表，按 id 倒序；limit 由调用方/存储层夹取。 */
    public List<SnapshotBackupRecord> listBackups(int limit) {
        return backupStore.list(limit);
    }

    /** 快照详情（含逐分区恢复 offset）；不存在时返回 {@code null}。 */
    public SnapshotBackupDetail getBackup(long id) {
        return backupStore.find(id);
    }

    // ------------------------------------------------------------------
    // 排空
    // ------------------------------------------------------------------

    private SnapshotPayload awaitQuiescenceAndCapture() {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(properties.drainTimeoutMs());
        int attempt = 0;
        while (true) {
            attempt++;
            if (fatalState.isFatal()) {
                throw new SnapshotUnavailableException(
                        "engine became fatal while draining for snapshot");
            }

            flushPipelines(deadline);
            if (internalTopicsDrained(deadline)) {
                ReentrantLock accountLock = accountBatchLock.reentrantLock();
                ReentrantLock orderLock = orderBatchLock.reentrantLock();
                boolean accountAcquired = false;
                boolean orderAcquired = false;
                try {
                    long moduleTimeout = Math.min(properties.moduleLockTimeoutMs(),
                            remainingMillis(deadline));
                    accountAcquired = tryLock(accountLock, moduleTimeout, "account module");
                    if (accountAcquired) {
                        moduleTimeout = Math.min(properties.moduleLockTimeoutMs(),
                                remainingMillis(deadline));
                        orderAcquired = tryLock(orderLock, moduleTimeout, "order module");
                        if (orderAcquired) {
                            // 两个模块都停下来后再 flush 一次：合并事务期间可能还有包在途。
                            flushPipelines(deadline);
                            if (internalTopicsDrained(deadline)) {
                                log.info("snapshot reached quiescence after {} attempt(s)", attempt);
                                return capture(deadline);
                            }
                        }
                    }
                } finally {
                    if (orderAcquired) {
                        orderLock.unlock();
                    }
                    if (accountAcquired) {
                        accountLock.unlock();
                    }
                }
            }

            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new SnapshotUnavailableException("snapshot drain timed out after "
                        + properties.drainTimeoutMs() + "ms");
            }
            if (attempt % 25 == 0) {
                log.info("snapshot still draining: attempt={}, inFlight(order={}, account={}), "
                                + "remaining={}ms",
                        attempt,
                        orderCommitPipeline.inFlightPackages(),
                        accountCommitPipeline.inFlightPackages(),
                        TimeUnit.NANOSECONDS.toMillis(remainingNanos));
            }
            sleep(Math.min(RETRY_DELAY_MS,
                    Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos))));
        }
    }

    private void flushPipelines(long deadline) {
        try {
            orderCommitPipeline.flush(remainingMillis(deadline));
            accountCommitPipeline.flush(remainingMillis(deadline));
        } catch (IllegalStateException e) {
            // committer fatal / 停止 / flush 超时：快照不可用，不能把未提交状态写进文件。
            throw new SnapshotUnavailableException(
                    "transaction committer flush failed: " + e.getMessage(), e);
        }
    }

    private boolean internalTopicsDrained(long deadline) {
        Map<TopicPartition, Long> ends = endOffsets(deadline);
        Map<TopicPartition, Long> lagging = new LinkedHashMap<>();
        for (Target target : internalTargets) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    committedOffsets(target.group(), deadline);
            for (int partition : partitionsOf(target.topic(), deadline)) {
                TopicPartition topicPartition = new TopicPartition(target.topic(), partition);
                long end = ends.getOrDefault(topicPartition, 0L);
                long next = committedOffset(committed, topicPartition);
                if (next < end) {
                    lagging.put(topicPartition, next);
                }
            }
        }
        if (lagging.isEmpty()) {
            return true;
        }
        return noDataRecordsInGap(lagging, ends, deadline);
    }

    /**
     * 用 read_committed 探针从 committed offset 开始读：如果只读到事务 marker
     * （consumer 会跳过，poll 返回空且 position 推进到 LSO），说明 gap 里没有数据记录；
     * 如果读到任何数据记录，说明还没排空。
     */
    private boolean noDataRecordsInGap(Map<TopicPartition, Long> lagging,
                                       Map<TopicPartition, Long> ends,
                                       long deadline) {
        KafkaConsumer<byte[], byte[]> probe = drainProbe();
        probe.assign(lagging.keySet());
        for (Map.Entry<TopicPartition, Long> entry : lagging.entrySet()) {
            probe.seek(entry.getKey(), entry.getValue());
        }

        long probeDeadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(Math.min(500L, remainingMillis(deadline)));
        while (true) {
            long remainingPollNanos = probeDeadline - System.nanoTime();
            if (remainingPollNanos <= 0) {
                // 探针窗口内没确认读完：按未排空处理，让外层循环重试。
                return false;
            }
            long pollMillis = Math.max(1L,
                    Math.min(100L, TimeUnit.NANOSECONDS.toMillis(remainingPollNanos)));
            ConsumerRecords<byte[], byte[]> records = probe.poll(Duration.ofMillis(pollMillis));
            if (!records.isEmpty()) {
                ConsumerRecord<byte[], byte[]> first = records.iterator().next();
                log.debug("snapshot drain found unconsumed data record: "
                                + "topic={}, partition={}, offset={}",
                        first.topic(), first.partition(), first.offset());
                return false;
            }
            boolean allAdvanced = true;
            for (TopicPartition topicPartition : lagging.keySet()) {
                if (probe.position(topicPartition)
                        < ends.getOrDefault(topicPartition, 0L)) {
                    allAdvanced = false;
                    break;
                }
            }
            if (allAdvanced) {
                return true;
            }
        }
    }

    private KafkaConsumer<byte[], byte[]> drainProbe() {
        KafkaConsumer<byte[], byte[]> probe = drainProbe;
        if (probe != null) {
            return probe;
        }
        synchronized (this) {
            if (drainProbe == null) {
                drainProbe = createDrainProbe();
            }
            return drainProbe;
        }
    }

    private KafkaConsumer<byte[], byte[]> createDrainProbe() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,
                "snapshot-drain-probe-" + kafkaProperties.instanceId());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "snapshot-drain-probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                kafkaProperties.consumer().isolationLevel());
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    @PreDestroy
    public void closeDrainProbe() {
        KafkaConsumer<byte[], byte[]> probe = drainProbe;
        drainProbe = null;
        if (probe != null) {
            try {
                probe.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("failed to close snapshot drain probe", e);
            }
        }
    }

    // ------------------------------------------------------------------
    // 静止点复制
    // ------------------------------------------------------------------

    private SnapshotPayload capture(long deadline) {
        List<AccountView> accounts = accountService.snapshotAccounts();
        OrderStateSnapshot orderState = orderService.snapshotState();
        List<OrderBookSnapshot> books = matchingEngine.snapshot();
        long accountSeq = accountOperationService.currentSeq();
        long orderSeq = orderOperationService.currentSeq();
        List<TopicPartitionOffset> inputOffsets = readInputOffsets(deadline);
        List<ReplayCheckpoint> replayCheckpoints = readReplayCheckpoints(deadline);
        return new SnapshotPayload(
                kafkaProperties.instanceId(),
                Instant.now().toString(),
                accountSeq,
                orderSeq,
                orderState,
                accounts,
                books,
                inputOffsets,
                replayCheckpoints);
    }

    private List<TopicPartitionOffset> readInputOffsets(long deadline) {
        List<TopicPartitionOffset> offsets = new ArrayList<>();
        for (Target target : inputTargets) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    committedOffsets(target.group(), deadline);
            for (int partition : partitionsOf(target.topic(), deadline)) {
                TopicPartition topicPartition = new TopicPartition(target.topic(), partition);
                offsets.add(new TopicPartitionOffset(
                        target.topic(), target.group(), partition,
                        committedOffset(committed, topicPartition)));
            }
        }
        offsets.sort(Comparator.comparing(TopicPartitionOffset::topic)
                .thenComparing(TopicPartitionOffset::group)
                .thenComparingInt(TopicPartitionOffset::partition));
        return List.copyOf(offsets);
    }

    /**
     * 读取两个 offset-seq topic 在快照切点的 end offset。
     *
     * <p>调用方已经 flush 两个 committer 并持有账户 / 委托模块锁，不会再有新的
     * offset-seq 记录提交；使用 {@code read_committed} 的 latest 位点，排除尚未提交的
     * 事务记录。恢复时从这里开始读映射，再用快照 seq 水位过滤。
     */
    private List<ReplayCheckpoint> readReplayCheckpoints(long deadline) {
        Map<TopicPartition, Long> ends = endOffsets(replayCheckpointTopics, deadline);
        List<ReplayCheckpoint> checkpoints = new ArrayList<>();
        for (String topic : replayCheckpointTopics) {
            for (int partition : partitionsOf(topic, deadline)) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                checkpoints.add(new ReplayCheckpoint(
                        topic, partition, ends.getOrDefault(topicPartition, 0L)));
            }
        }
        checkpoints.sort(Comparator.comparing(ReplayCheckpoint::topic)
                .thenComparingInt(ReplayCheckpoint::partition));
        return List.copyOf(checkpoints);
    }

    /**
     * 校验「每 (userId, currency) 的未成交委托剩余冻结 == 账户冻结」。
     *
     * <p>只有真正静止的切点才能满足：ACCEPTED 委托（等待冻结回执）说明内部 topic
     * 还没排空；剩余冻结为负或两边不等，说明内存状态或 offset 切点有问题。校验失败
     * 时不写文件。快照加载端复用同一份校验。
     */
    private void validateInvariant(SnapshotPayload payload) {
        try {
            SnapshotInvariantValidator.validate(payload.accounts(), payload.orderState());
        } catch (IllegalStateException e) {
            throw new SnapshotUnavailableException(e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Kafka 元数据
    // ------------------------------------------------------------------

    private Map<TopicPartition, Long> endOffsets(long deadline) {
        return endOffsets(internalTargets.stream().map(Target::topic).toList(), deadline);
    }

    private Map<TopicPartition, Long> endOffsets(List<String> topics, long deadline) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (String topic : topics) {
            for (int partition : partitionsOf(topic, deadline)) {
                request.put(new TopicPartition(topic, partition), OffsetSpec.latest());
            }
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> infos = call(
                adminClient.listOffsets(request,
                                new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                        .all(),
                deadline, "end offsets");
        Map<TopicPartition, Long> result = new HashMap<>(infos.size());
        infos.forEach((topicPartition, info) ->
                result.put(topicPartition, info.offset()));
        return result;
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(String group, long deadline) {
        try {
            return adminClient.listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata()
                    .get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SnapshotUnavailableException(
                    "interrupted while reading committed offsets for group " + group);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                // 组还没有提交过 offset：所有分区都按 offset 0 处理。
                return Map.of();
            }
            throw new SnapshotUnavailableException(
                    "failed to read committed offsets for group " + group
                            + ": " + e.getCause(), e);
        } catch (TimeoutException e) {
            throw new SnapshotUnavailableException(
                    "timed out reading committed offsets for group " + group);
        }
    }

    private List<Integer> partitionsOf(String topic, long deadline) {
        List<Integer> cached = partitionCache.get(topic);
        if (cached != null) {
            return cached;
        }
        synchronized (partitionCache) {
            cached = partitionCache.get(topic);
            if (cached != null) {
                return cached;
            }
            Map<String, TopicDescription> descriptions = call(
                    adminClient.describeTopics(List.of(topic)).allTopicNames(),
                    deadline, "topic description for " + topic);
            TopicDescription description = descriptions.get(topic);
            if (description == null) {
                throw new SnapshotUnavailableException("topic not found: " + topic);
            }
            List<Integer> partitions = description.partitions().stream()
                    .map(partition -> partition.partition())
                    .sorted()
                    .toList();
            partitionCache.put(topic, partitions);
            return partitions;
        }
    }

    private static long committedOffset(Map<TopicPartition, OffsetAndMetadata> committed,
                                        TopicPartition topicPartition) {
        OffsetAndMetadata offset = committed.get(topicPartition);
        return offset == null ? 0L : offset.offset();
    }

    private static <T> T call(KafkaFuture<T> future, long deadline, String action) {
        try {
            return future.get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SnapshotUnavailableException("interrupted while reading " + action);
        } catch (ExecutionException e) {
            throw new SnapshotUnavailableException(
                    "failed to read " + action + ": " + e.getCause(), e);
        } catch (TimeoutException e) {
            throw new SnapshotUnavailableException("timed out reading " + action);
        }
    }

    // ------------------------------------------------------------------
    // 锁与辅助
    // ------------------------------------------------------------------

    private void acquireEntryLocks(List<ReentrantLock> locks) {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(properties.entryLockTimeoutMs());
        List<ReentrantLock> acquired = new ArrayList<>(locks.size());
        try {
            for (ReentrantLock lock : locks) {
                if (!tryLock(lock, remainingMillis(deadline), "entry")) {
                    throw new SnapshotUnavailableException(
                            "timed out acquiring snapshot entry locks after "
                                    + properties.entryLockTimeoutMs() + "ms");
                }
                acquired.add(lock);
            }
        } finally {
            if (acquired.size() != locks.size()) {
                releaseLocks(acquired);
            }
        }
    }

    private static boolean tryLock(ReentrantLock lock, long timeoutMillis, String name) {
        try {
            return lock.tryLock(Math.max(1L, timeoutMillis), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SnapshotUnavailableException(
                    "interrupted while acquiring " + name + " lock");
        }
    }

    private static void releaseLocks(List<ReentrantLock> locks) {
        for (int index = locks.size() - 1; index >= 0; index--) {
            locks.get(index).unlock();
        }
    }

    private static long remainingMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new SnapshotUnavailableException("snapshot operation timed out");
        }
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SnapshotUnavailableException(
                    "interrupted while waiting for snapshot quiescence");
        }
    }

    private Target target(String topicKey, String groupKey) {
        String topic = topic(topicKey);
        String group = kafkaProperties.groups().get(groupKey);
        if (group == null || group.isBlank()) {
            throw new IllegalStateException("missing app.kafka.groups." + groupKey);
        }
        return new Target(topic, group);
    }

    private String topic(String topicKey) {
        String topic = kafkaProperties.topics().get(topicKey);
        if (topic == null || topic.isBlank()) {
            throw new IllegalStateException("missing app.kafka.topics." + topicKey);
        }
        return topic;
    }

    private record Target(String topic, String group) {
    }
}
