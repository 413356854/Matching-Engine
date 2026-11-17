package com.me.snapshot.recovery;

import com.me.account.service.AccountOperationService;
import com.me.account.service.AccountService;
import com.me.kafka.KafkaClientProperties;
import com.me.matching.service.MatchingEngine;
import com.me.order.service.OrderOperationService;
import com.me.order.service.OrderService;
import com.me.snapshot.SnapshotBackupStore;
import com.me.snapshot.SnapshotLoader;
import com.me.snapshot.SnapshotRecoveryException;
import com.me.snapshot.SnapshotRecoveryProperties;
import com.me.snapshot.dto.ReplayCheckpoint;
import com.me.snapshot.dto.SnapshotBackupDetail;
import com.me.snapshot.dto.SnapshotBackupRecord;
import com.me.snapshot.dto.SnapshotPayload;
import com.me.snapshot.dto.TopicPartitionOffset;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

/**
 * 启动恢复编排：加载快照 → 恢复内存 → 重放快照切点之后的已提交输入 → 对齐消费 offset。
 *
 * <h2>恢复要解决的问题</h2>
 * 快照只覆盖它生成那一刻的内存状态。进程在快照之后又处理了一批输入（输出、消费 offset、
 * offset-seq 映射已在同一个 Kafka 事务里提交），随后崩溃；重启时内存已经丢失，但 Kafka
 * 里已经有「快照之后的已提交进度」。恢复必须把内存补到「最后一次已提交的输入」，
 * 同时不能把已经提交过的输出再发一遍（账户结算、资金流水都不幂等）。
 *
 * <h2>整体流程</h2>
 * <ol>
 *   <li><b>选快照</b>：最新记录 / 指定 backup id / 显式文件；</li>
 *   <li><b>读文件并校验</b>：格式版本、SHA-256、账户/委托/订单簿引用关系、冻结不变量；</li>
 *   <li><b>校验拓扑</b>：快照记录的 topic 分区数、消费组必须与当前配置一致；</li>
 *   <li><b>恢复内存</b>：账户、委托、订单簿、accountSeq/orderSeq 水位、结算 ID 生成器；</li>
 *   <li><b>读重放映射</b>：从两个 offset-seq topic 的 checkpoint 之后读出
 *       「输入记录 → 全局 seq」；</li>
 *   <li><b>推导 offset 计划</b>：对比快照 offset S 和 broker committed offset C，确定
 *       重放窗口 [S, C) 以及哪些组需要写回快照位点；</li>
 *   <li><b>严格校验</b>：扫描 [S, C) 的可见输入记录，要求与 offset-seq 映射一一对应；</li>
 *   <li><b>状态重放</b>：按 seq 顺序、保留原始批次边界调用现有 service，输出全部丢弃；</li>
 *   <li><b>对齐消费 offset</b>：只把缺失 / 落后的组写回快照位点，其余保持 broker 位点；</li>
 *   <li><b>放行</b>：标记 READY；随后 Spring 启动 committer 和 consumer，从 C 继续消费。</li>
 * </ol>
 *
 * <h2>关键不变量</h2>
 * <ul>
 *   <li>重放只改内存，不发布任何 Kafka 输出；</li>
 *   <li>每批重放后校验 service 新分配的 seq / 输入位置与 offset-seq 完全一致；</li>
 *   <li>崩溃前未提交的批次没有映射、输入 offset 也未提交，启动后由正常消费链路重投；</li>
 *   <li>只有严格覆盖校验通过后才允许放行 consumer。</li>
 * </ul>
 *
 * <p>phase=10，早于 {@code ModuleCommitPipeline(50)} 和 {@code KafkaConsumerManager(100)}。
 * 重放使用 discard 输出，不依赖 committer；任何失败都会阻止 consumer 启动，避免带着
 * 部分状态对外服务。
 */
@Service
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class SnapshotRecoveryService implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SnapshotRecoveryService.class);
    private static final long OFFSET_RETRY_DELAY_MS = 500L;

    private final SnapshotRecoveryProperties properties;
    private final SnapshotLoader loader;
    private final SnapshotBackupStore backupStore;
    private final AdminClient adminClient;
    private final KafkaClientProperties kafkaProperties;
    private final AccountService accountService;
    private final OrderService orderService;
    private final MatchingEngine matchingEngine;
    private final AccountOperationService accountOperationService;
    private final OrderOperationService orderOperationService;
    private final OffsetSeqReplayReader replayReader;
    private final ReplayInputFetcher inputFetcher;
    private final ReplayApplier replayApplier;
    private final RecoveryState recoveryState;

    private final AtomicBoolean running = new AtomicBoolean();

    public SnapshotRecoveryService(
            SnapshotRecoveryProperties properties,
            SnapshotLoader loader,
            SnapshotBackupStore backupStore,
            @Qualifier("snapshotAdminClient") AdminClient adminClient,
            KafkaClientProperties kafkaProperties,
            AccountService accountService,
            OrderService orderService,
            MatchingEngine matchingEngine,
            AccountOperationService accountOperationService,
            OrderOperationService orderOperationService,
            OffsetSeqReplayReader replayReader,
            ReplayInputFetcher inputFetcher,
            ReplayApplier replayApplier,
            RecoveryState recoveryState) {
        this.properties = properties;
        this.loader = loader;
        this.backupStore = backupStore;
        this.adminClient = adminClient;
        this.kafkaProperties = kafkaProperties;
        this.accountService = accountService;
        this.orderService = orderService;
        this.matchingEngine = matchingEngine;
        this.accountOperationService = accountOperationService;
        this.orderOperationService = orderOperationService;
        this.replayReader = replayReader;
        this.inputFetcher = inputFetcher;
        this.replayApplier = replayApplier;
        this.recoveryState = recoveryState;
    }

    /**
     * Spring SmartLifecycle 入口。
     *
     * <p>恢复关闭时直接返回，保持“空状态启动”的既有行为；恢复开启时同步阻塞到恢复完成，
     * 任何异常都向上抛出，让 Spring 启动失败。这样 consumer / committer 永远不会在
     * 未恢复或部分恢复的状态上启动。
     *
     * <p>{@link RecoveryState} 在恢复完成前保持 PENDING / RUNNING，HTTP 过滤器据此
     * 返回 503，避免外部读到恢复中的半状态。
     */
    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        if (!properties.enabled()) {
            log.info("snapshot recovery is disabled; starting with empty state");
            return;
        }
        recoveryState.running("selecting snapshot");
        try {
            RecoveryStatus status = recover();
            recoveryState.ready(status);
            log.info("snapshot recovery ready: {}", status);
        } catch (RuntimeException e) {
            recoveryState.failed(e.getMessage());
            log.error("snapshot recovery failed; refusing to start consumers", e);
            throw e;
        }
    }

    /**
     * 执行一次完整恢复。
     *
     * <p>顺序是刻意安排的：
     * <ul>
     *   <li>先恢复内存、再读映射和 offset：映射校验失败时不会留下“offset 已改、状态没改”
     *       的组；</li>
     *   <li>先重放状态、最后写 offset：即使重放失败，consumer 也不会带着旧 offset 启动；</li>
     *   <li>严格覆盖校验在尾部重放之前：确认重放窗口完整后，才把尾部输入应用到内存。</li>
     * </ul>
     *
     * @return READY 状态；没找到可用快照且 fail-if-missing=false 时返回空状态 READY
     */
    private RecoveryStatus recover() {
        String startedAt = Instant.now().toString();
        Selection selection = selectSnapshot();
        if (selection == null) {
            // 没找到快照且配置允许空启动：保持既有“空状态启动”行为。
            return RecoveryStatus.initial("READY", true,
                    "no usable snapshot; starting with empty state");
        }

        // 1) 文件级校验：格式、校验和、账户/委托/订单簿结构与冻结不变量。
        recoveryState.running("loading snapshot " + selection.path());
        SnapshotLoader.LoadedSnapshot loaded = loader.load(selection.path());
        SnapshotPayload payload = loaded.payload();
        if (selection.record() != null) {
            // 文件可信后，再和 MySQL 元数据交叉校验，防止文件和记录不是同一次快照。
            validateMetadata(selection.record(), loaded);
        }

        // 2) 运行期拓扑校验：分区数、消费组必须与快照一致，否则输入记录无法定位。
        validateTopology(payload);

        // 3) 恢复内存。此时 consumer / committer 都还没启动，也没有 HTTP 请求能进入业务。
        recoveryState.running("restoring in-memory state");
        accountService.restoreAccounts(payload.accounts());
        orderService.restoreState(payload.orderState());
        matchingEngine.restore(payload.books());
        accountOperationService.restoreSeq(payload.accountSeq());
        orderOperationService.restoreSeq(payload.orderSeq());
        log.info("in-memory state restored: accounts={}, orders={}, books={}, "
                        + "accountSeq={}, orderSeq={}",
                payload.accounts().size(), payload.orderState().orders().size(),
                payload.books().size(), payload.accountSeq(), payload.orderSeq());

        try {
            // 4) 读出快照切点之后已提交的「输入记录 → seq」映射，保留原始批次边界。
            recoveryState.running("reading offset-seq replay mappings");
            ReplayMappings mappings = replayReader.read(payload);
            List<ReplayMapping> allMappings = mappings.allMappings();
            MappingStats mappingStats = mappingStats(payload.inputOffsets(), allMappings);

            // 5) 对比快照 offset 与 broker committed offset，得到每个分区的重放窗口和
            //    最终消费位点。C > S 的分区必须有覆盖到 C-1 的映射。
            Map<OffsetKey, Long> brokerOffsets = readBrokerOffsets(payload.inputOffsets());
            validateInputLogBounds(payload.inputOffsets(), brokerOffsets);
            RecoveryOffsetPlan offsetPlan = RecoveryOffsetPlanner.plan(
                    payload.inputOffsets(), brokerOffsets,
                    mappingStats.mappedCounts(), mappingStats.maxMappedOffsets(),
                    allMappings);
            logOffsetPlan(offsetPlan);

            // 6) 严格模式扫描 [S, C) 的可见输入记录：不能有记录没映射，也不能有映射
            //    找不到记录。防止 offset-seq / 输入 topic 被 retention 删除后静默少重放。
            if (properties.strictValidation()) {
                recoveryState.running("validating replay coverage");
                validateCoverage(offsetPlan, allMappings);
            }

            // 7) 只改状态、不发输出。每批校验 seq 与输入位置，任何偏差都中止恢复。
            recoveryState.running("replaying state");
            ReplayApplier.ReplayStats replayStats = replayApplier.replay(mappings);

            // 8) 重放成功后才对齐消费 offset。正常情况（映射完整、broker offset 领先）
            //    这里不会写任何 offset，consumer 直接从 broker 位点继续。
            recoveryState.running("aligning consumer offsets");
            int adjustedGroups = applyOffsetAdjustments(offsetPlan);

            // 9) 最终水位校验：重放必须至少推进到快照 watermark。
            if (orderOperationService.currentSeq() < payload.orderSeq()
                    || accountOperationService.currentSeq() < payload.accountSeq()) {
                throw new SnapshotRecoveryException("replay did not advance seq counters to "
                        + "the snapshot watermarks: orderSeq="
                        + orderOperationService.currentSeq() + " (snapshot "
                        + payload.orderSeq() + "), accountSeq="
                        + accountOperationService.currentSeq() + " (snapshot "
                        + payload.accountSeq() + ")");
            }

            String message = "snapshot recovery completed: orderRecords="
                    + replayStats.orderRecords() + ", accountRecords="
                    + replayStats.accountRecords() + ", adjustedGroups=" + adjustedGroups;
            log.info(message);
            return new RecoveryStatus("READY", true, selection.backupId(),
                    selection.path().toString(), payload.createdAt(),
                    payload.accountSeq(), payload.orderSeq(),
                    payload.accounts().size(), payload.orderState().orders().size(),
                    payload.books().size(),
                    replayStats.orderBatches(), replayStats.orderRecords(),
                    replayStats.accountBatches(), replayStats.accountRecords(),
                    adjustedGroups, startedAt, Instant.now().toString(), message);
        } finally {
            // 抓取 consumer 使用独立 group + assign，不会提交 offset；恢复结束统一关闭。
            inputFetcher.close();
        }
    }

    // ------------------------------------------------------------------
    // 快照选择与元数据
    // ------------------------------------------------------------------

    /**
     * 选择要加载的快照。
     *
     * <p>优先级：显式文件 > 指定 backup id > 最新一条记录。显式文件用于 MySQL 元数据
     * 丢失的灾备场景，此时没有 DB 记录可交叉校验；指定 id 用于回滚到某个历史快照
     * （只要输入和 offset-seq 的 retention 还覆盖它之后的记录）。
     *
     * <p>找不到时由 {@link #missing(String)} 决定是失败还是空状态启动。
     */
    private Selection selectSnapshot() {
        if (!properties.file().isBlank()) {
            Path path = Path.of(properties.file());
            log.info("loading snapshot from explicit file: {}", path);
            return new Selection(path, null, null);
        }

        SnapshotBackupDetail detail;
        if (properties.backupId() > 0) {
            detail = backupStore.find(properties.backupId());
            if (detail == null) {
                return missing("snapshot backup id " + properties.backupId() + " was not found");
            }
        } else {
            // 按 id 倒序取最新一条；取详情是为了校验主表和 offset 表都存在。
            List<SnapshotBackupRecord> latest = backupStore.list(1);
            if (latest.isEmpty()) {
                return missing("snapshot_backup has no records");
            }
            detail = backupStore.find(latest.get(0).id());
            if (detail == null) {
                return missing("snapshot backup id " + latest.get(0).id()
                        + " disappeared while loading");
            }
        }

        SnapshotBackupRecord record = detail.backup();
        if (record.filePath() == null || record.filePath().isBlank()) {
            throw new SnapshotRecoveryException(
                    "snapshot backup " + record.id() + " has no file path");
        }
        log.info("selected snapshot backup: id={}, file={}, accountSeq={}, orderSeq={}",
                record.id(), record.filePath(), record.accountSeq(), record.orderSeq());
        return new Selection(Path.of(record.filePath()), record.id(), record);
    }

    /**
     * 没有可用快照时的处理策略。
     *
     * <p>默认 fail-if-missing=true：开启恢复却找不到快照通常意味着路径/表/文件出了问题，
     * 继续空状态启动会让系统在丢失全部内存状态的情况下对外服务，因此直接失败。
     * 显式配置 false 时才退化为既有空状态启动。
     */
    private Selection missing(String reason) {
        if (properties.failIfMissing()) {
            throw new SnapshotRecoveryException(reason
                    + "; snapshot recovery is enabled with fail-if-missing=true");
        }
        log.warn("{}; starting with empty state", reason);
        return null;
    }

    /**
     * 交叉校验 MySQL 记录与快照文件内容。
     *
     * <p>文件本身已经通过 SHA-256 自校验，但 DB 记录可能指向另一份文件（手工替换、
     * 备份恢复错文件、目录被覆盖）。这里比对 checksum、seq 水位、各类数量、文件大小，
     * 避免“记录说 A 版本、实际加载 B 版本”。
     *
     * <p>instanceId 只告警不失败：单实例部署时实例名可能随重启变化，不影响状态语义。
     */
    private void validateMetadata(SnapshotBackupRecord record,
                                  SnapshotLoader.LoadedSnapshot loaded) {
        SnapshotPayload payload = loaded.payload();
        if (record.formatVersion() != loaded.file().formatVersion()) {
            throw new SnapshotRecoveryException("snapshot metadata formatVersion "
                    + record.formatVersion() + " does not match file "
                    + loaded.file().formatVersion());
        }
        if (record.checksum() != null
                && !record.checksum().equalsIgnoreCase(loaded.file().checksum())) {
            throw new SnapshotRecoveryException("snapshot metadata checksum does not match file "
                    + loaded.path());
        }
        if (record.accountSeq() != payload.accountSeq()
                || record.orderSeq() != payload.orderSeq()) {
            throw new SnapshotRecoveryException("snapshot metadata seq does not match payload: "
                    + "metadata accountSeq=" + record.accountSeq() + ", orderSeq="
                    + record.orderSeq() + "; payload accountSeq=" + payload.accountSeq()
                    + ", orderSeq=" + payload.orderSeq());
        }
        if (record.accountCount() != payload.accounts().size()
                || record.orderCount() != payload.orderState().orders().size()
                || record.bookCount() != payload.books().size()
                || record.offsetCount() != payload.inputOffsets().size()) {
            throw new SnapshotRecoveryException("snapshot metadata counts do not match payload");
        }
        if (record.fileSizeBytes() > 0) {
            try {
                long actual = Files.size(loaded.path());
                if (actual != record.fileSizeBytes()) {
                    throw new SnapshotRecoveryException("snapshot file size mismatch: metadata="
                            + record.fileSizeBytes() + ", actual=" + actual);
                }
            } catch (IOException e) {
                throw new SnapshotRecoveryException("failed to read snapshot file size for "
                        + loaded.path(), e);
            }
        }
        if (record.instanceId() != null && !record.instanceId().isBlank()
                && !record.instanceId().equals(payload.instanceId())) {
            log.warn("snapshot instanceId {} differs from snapshot payload {}; continuing",
                    record.instanceId(), payload.instanceId());
        }
    }

    // ------------------------------------------------------------------
    // Kafka 拓扑校验
    // ------------------------------------------------------------------

    /**
     * 校验快照记录的 Kafka 拓扑与当前配置一致。
     *
     * <p>快照里的 offset 只在「同一个 topic、同一个分区数、同一个消费组」下有意义：
     * <ul>
     *   <li>消费组变了：offset 会被写到另一个组，原组继续按旧位点消费；</li>
     *   <li>分区数变了：快照没有新分区的 offset，重放窗口无法定义；</li>
     *   <li>topic 名变了：快照里的输入记录根本不在当前消费链路里。</li>
     * </ul>
     * 因此这里做严格相等校验，而不是尽量兼容。
     */
    private void validateTopology(SnapshotPayload payload) {
        List<ExpectedTarget> expected = expectedInputTargets();
        Set<String> expectedTopics = new HashSet<>();
        expected.forEach(target -> expectedTopics.add(target.topic()));
        String orderOffsetSeq = topic("order-offset-seq");
        String accountOffsetSeq = topic("account-offset-seq");
        expectedTopics.add(orderOffsetSeq);
        expectedTopics.add(accountOffsetSeq);

        Map<String, Integer> partitionCounts = describePartitionCounts(expectedTopics);
        validateInputOffsets(payload.inputOffsets(), expected, partitionCounts);
        validateReplayCheckpoints(payload.replayCheckpoints(),
                Map.of(orderOffsetSeq, partitionCounts.get(orderOffsetSeq),
                        accountOffsetSeq, partitionCounts.get(accountOffsetSeq)));
    }

    /**
     * 状态机的六条输入链路，以及它们与消费组的固定绑定。
     *
     * <p>顺序与 {@code SnapshotService.inputTargets} 保持一致；消费组名称来自
     * {@code app.kafka.groups}，必须与快照记录、Kafka 实际提交 offset 的组一致。
     */
    private List<ExpectedTarget> expectedInputTargets() {
        return List.of(
                new ExpectedTarget(topic("order-commands"), group("order")),
                new ExpectedTarget(topic("order-cancels"), group("order-cancel")),
                new ExpectedTarget(topic("order-freeze-results"), group("order-freeze-result")),
                new ExpectedTarget(topic("account-commands"), group("account")),
                new ExpectedTarget(topic("account-order-freeze-commands"),
                        group("account-order-freeze")),
                new ExpectedTarget(topic("account-settlements"), group("account-settlement")));
    }

    /**
     * 读取 topic 分区数。
     *
     * <p>{@link com.me.kafka.KafkaTopicInitializer} 在 phase 0 异步建 topic，可能还没完成；
     * 恢复需要这些 topic，因此在 admin 超时内轮询等待 UnknownTopicOrPartition。
     */
    private Map<String, Integer> describePartitionCounts(Set<String> topics) {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(properties.adminTimeoutMs());
        while (true) {
            try {
                Map<String, TopicDescription> descriptions = adminClient
                        .describeTopics(topics)
                        .allTopicNames()
                        .get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
                Map<String, Integer> counts = new HashMap<>(descriptions.size() * 2);
                descriptions.forEach((topic, description) ->
                        counts.put(topic, description.partitions().size()));
                return counts;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SnapshotRecoveryException("interrupted while describing topics");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException
                        && System.nanoTime() < deadline) {
                    log.info("snapshot recovery is waiting for topics to be created: {}",
                            e.getCause().getMessage());
                    sleep(OFFSET_RETRY_DELAY_MS);
                    continue;
                }
                throw new SnapshotRecoveryException(
                        "failed to describe snapshot topics: " + e.getCause(), e);
            } catch (TimeoutException e) {
                throw new SnapshotRecoveryException(
                        "timed out describing snapshot topics", e);
            }
        }
    }

    /**
     * 校验六条输入 topic 的每个分区都有恢复 offset，且没有多余 / 未知记录。
     *
     * <p>快照写入了每个分区的 committed offset S。恢复时：
     * <ul>
     *   <li>缺少某个分区：该分区从哪开始消费没有依据；</li>
     *   <li>多出未知 topic / 分区：快照与当前拓扑不一致；</li>
     *   <li>消费组不匹配：offset 会落到错误的组。</li>
     * </ul>
     * 因此要求「快照 offset 集合 == 当前 topic×分区集合」。
     */
    private void validateInputOffsets(List<TopicPartitionOffset> offsets,
                                      List<ExpectedTarget> expected,
                                      Map<String, Integer> partitionCounts) {
        Map<String, ExpectedTarget> byTopic = new HashMap<>();
        for (ExpectedTarget target : expected) {
            byTopic.put(target.topic(), target);
            int partitions = partitionCounts.getOrDefault(target.topic(), 0);
            if (partitions <= 0) {
                throw new SnapshotRecoveryException("snapshot input topic not found: "
                        + target.topic());
            }
        }

        Set<String> seen = new HashSet<>();
        for (TopicPartitionOffset offset : offsets) {
            ExpectedTarget target = byTopic.get(offset.topic());
            if (target == null) {
                throw new SnapshotRecoveryException("snapshot references unexpected input topic "
                        + offset.topic());
            }
            if (!target.group().equals(offset.group())) {
                throw new SnapshotRecoveryException("snapshot input topic " + offset.topic()
                        + " uses consumer group " + offset.group() + " but the running config "
                        + "expects " + target.group());
            }
            int partitions = partitionCounts.getOrDefault(offset.topic(), 0);
            if (offset.partition() >= partitions) {
                throw new SnapshotRecoveryException("snapshot input offset " + offset.topic()
                        + "/" + offset.partition() + " exceeds current partition count "
                        + partitions);
            }
            String key = offset.topic() + "|" + offset.group() + "|" + offset.partition();
            if (!seen.add(key)) {
                throw new SnapshotRecoveryException("duplicate snapshot input offset: " + key);
            }
        }

        for (ExpectedTarget target : expected) {
            int partitions = partitionCounts.get(target.topic());
            for (int partition = 0; partition < partitions; partition++) {
                String key = target.topic() + "|" + target.group() + "|" + partition;
                if (!seen.contains(key)) {
                    throw new SnapshotRecoveryException("snapshot is missing input offset "
                            + key);
                }
            }
        }
    }

    /**
     * 校验两个 offset-seq topic 的 checkpoint 覆盖当前全部分区。
     *
     * <p>checkpoint 是读取重放映射的起点。缺少某个分区会让该分区在快照之后的映射
     * 被静默漏掉，导致重放状态少于已提交状态，因此要求完整覆盖。
     */
    private void validateReplayCheckpoints(List<ReplayCheckpoint> checkpoints,
                                           Map<String, Integer> partitionCounts) {
        for (Map.Entry<String, Integer> entry : partitionCounts.entrySet()) {
            if (entry.getValue() == null || entry.getValue() <= 0) {
                throw new SnapshotRecoveryException(
                        "offset-seq topic not found: " + entry.getKey());
            }
        }
        Set<String> seen = new HashSet<>();
        for (ReplayCheckpoint checkpoint : checkpoints) {
            Integer partitions = partitionCounts.get(checkpoint.topic());
            if (partitions == null) {
                throw new SnapshotRecoveryException(
                        "unexpected replay checkpoint topic: " + checkpoint.topic());
            }
            if (checkpoint.partition() >= partitions) {
                throw new SnapshotRecoveryException("replay checkpoint " + checkpoint.topic()
                        + "/" + checkpoint.partition() + " exceeds partition count " + partitions);
            }
            String key = checkpoint.topic() + "|" + checkpoint.partition();
            if (!seen.add(key)) {
                throw new SnapshotRecoveryException("duplicate replay checkpoint: " + key);
            }
        }
        for (Map.Entry<String, Integer> entry : partitionCounts.entrySet()) {
            for (int partition = 0; partition < entry.getValue(); partition++) {
                String key = entry.getKey() + "|" + partition;
                if (!seen.contains(key)) {
                    throw new SnapshotRecoveryException(
                            "snapshot is missing replay checkpoint " + key);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 映射统计、broker offset、覆盖校验
    // ------------------------------------------------------------------

    /**
     * 按「消费组 + topic + 分区」汇总重放映射的条数和最大输入 offset。
     *
     * <p>这两个统计量用于判断尾部是否完整：
     * <ul>
     *   <li>broker committed offset 为 C 时，该分区最后一条已处理记录的 offset 必须是
     *       C-1，否则说明还有已提交记录没有映射；</li>
     *   <li>映射条数用于严格覆盖校验和恢复日志。</li>
     * </ul>
     */
    private MappingStats mappingStats(List<TopicPartitionOffset> inputOffsets,
                                      List<ReplayMapping> mappings) {
        Map<String, String> groupByTopic = groupByTopic(inputOffsets);
        Map<OffsetKey, Long> counts = new HashMap<>();
        Map<OffsetKey, Long> maxOffsets = new HashMap<>();
        for (ReplayMapping mapping : mappings) {
            String group = groupByTopic.get(mapping.topic());
            if (group == null) {
                throw new SnapshotRecoveryException("replay mapping references unknown input "
                        + "topic " + mapping.topic());
            }
            OffsetKey key = new OffsetKey(group, mapping.topic(), mapping.partition());
            counts.merge(key, 1L, Long::sum);
            maxOffsets.merge(key, mapping.offset(), Math::max);
        }
        return new MappingStats(counts, maxOffsets);
    }

    /**
     * 读取六条输入链路当前在 broker 上的 committed offset C。
     *
     * <p>C 表示「该消费组最后确认处理到的下一条记录」。恢复用它确定重放窗口的右边界：
     * C > S 的部分是快照之后已提交的输入，需要重放；C == S 表示没有尾部；C 缺失表示
     * 组从未提交过（可能是首次启动或位点被清理），由 {@link RecoveryOffsetPlanner}
     * 决定写回快照位点。
     *
     * <p>这里不修改任何 offset，只读取；写回在重放成功后的
     * {@link #applyOffsetAdjustments(RecoveryOffsetPlan)} 里做。
     */
    private Map<OffsetKey, Long> readBrokerOffsets(List<TopicPartitionOffset> inputOffsets) {
        Map<String, Set<TopicPartition>> byGroup = new LinkedHashMap<>();
        for (TopicPartitionOffset offset : inputOffsets) {
            byGroup.computeIfAbsent(offset.group(), ignored -> new HashSet<>())
                    .add(new TopicPartition(offset.topic(), offset.partition()));
        }

        Map<OffsetKey, Long> brokerOffsets = new HashMap<>();
        for (Map.Entry<String, Set<TopicPartition>> entry : byGroup.entrySet()) {
            String group = entry.getKey();
            Map<TopicPartition, OffsetAndMetadata> committed;
            try {
                committed = adminClient.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata()
                        .get(properties.adminTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SnapshotRecoveryException(
                        "interrupted while reading committed offsets for group " + group);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof GroupIdNotFoundException) {
                    committed = Map.of();
                } else {
                    throw new SnapshotRecoveryException(
                            "failed to read committed offsets for group " + group
                                    + ": " + e.getCause(), e);
                }
            } catch (TimeoutException e) {
                throw new SnapshotRecoveryException(
                        "timed out reading committed offsets for group " + group);
            }

            for (TopicPartition partition : entry.getValue()) {
                OffsetAndMetadata offset = committed.get(partition);
                if (offset != null) {
                    brokerOffsets.put(new OffsetKey(
                            group, partition.topic(), partition.partition()), offset.offset());
                }
            }
        }
        return brokerOffsets;
    }

    /**
     * 快照 offset / broker committed offset 必须落在输入 topic 的 [logStart, logEnd] 内。
     *
     * <p>超过 logEnd 说明日志被截断或恢复了别的集群的快照；低于 logStart 说明需要重放的
     * 记录已被 retention 删除。两种情况都不能继续，否则 consumer 会按 auto.offset.reset
     * 从头重放，直接污染内存状态。
     */
    private void validateInputLogBounds(List<TopicPartitionOffset> inputOffsets,
                                        Map<OffsetKey, Long> brokerOffsets) {
        Map<TopicPartition, Long> starts = readInputLogOffsets(inputOffsets, OffsetSpec.earliest());
        Map<TopicPartition, Long> ends = readInputLogOffsets(inputOffsets, OffsetSpec.latest());

        for (TopicPartitionOffset offset : inputOffsets) {
            TopicPartition partition = new TopicPartition(offset.topic(), offset.partition());
            Long start = starts.get(partition);
            Long end = ends.get(partition);
            if (start == null || end == null) {
                throw new SnapshotRecoveryException(
                        "missing log bounds for input partition " + partition);
            }
            // S 超出 logEnd：日志被截断，或加载了别的集群 / 别的实例的快照。
            if (offset.nextOffset() > end) {
                throw new SnapshotRecoveryException("snapshot input offset "
                        + offset.nextOffset() + " for " + partition
                        + " is beyond the current log end " + end
                        + "; input data was truncated or the snapshot belongs to another cluster");
            }
            Long broker = brokerOffsets.get(
                    new OffsetKey(offset.group(), offset.topic(), offset.partition()));
            // C 超出 logEnd：组位点与当前日志不匹配，继续配置会触发 auto.offset.reset。
            if (broker != null && broker > end) {
                throw new SnapshotRecoveryException("committed offset " + broker + " for "
                        + partition + " is beyond the current log end " + end);
            }
            // C > S 表示存在需要重放的尾部；如果 logStart 已经越过 S，说明尾部记录
            // 被 retention 删除，重放无法完整复现状态。
            if (broker != null && broker > offset.nextOffset() && start > offset.nextOffset()) {
                throw new SnapshotRecoveryException("input records after the snapshot offset "
                        + offset.nextOffset() + " were deleted from " + partition
                        + " (log start " + start + ", committed " + broker
                        + "); replay cannot be completed");
            }
        }
    }

    /**
     * 批量读取输入 topic 的 log start / log end（read_committed 的 LSO）。
     *
     * <p>用一次 admin 请求覆盖所有分区，减少启动时的往返；latest 使用 READ_COMMITTED，
     * 排除尚未提交的事务记录，和 consumer 实际能看到的数据范围一致。
     */
    private Map<TopicPartition, Long> readInputLogOffsets(
            List<TopicPartitionOffset> inputOffsets, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (TopicPartitionOffset offset : inputOffsets) {
            request.put(new TopicPartition(offset.topic(), offset.partition()), spec);
        }
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> infos =
                    adminClient.listOffsets(request,
                                    new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                            .all()
                            .get(properties.adminTimeoutMs(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
            infos.forEach((partition, info) -> offsets.put(partition, info.offset()));
            return offsets;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SnapshotRecoveryException(
                    "interrupted while reading input topic log bounds");
        } catch (ExecutionException | TimeoutException e) {
            throw new SnapshotRecoveryException(
                    "failed to read input topic log bounds: " + e, e);
        }
    }

    /**
     * 严格覆盖校验：重放窗口 [S, C) 内的可见输入记录必须与 offset-seq 映射一一对应。
     *
     * <p>为什么需要：offset 计划只校验「最大映射 offset == C-1」，如果中间有映射被
     * retention 删除、或某个批次的 offset-seq 消息丢失，状态会静默少重放一段。这里
     * 对每个需要重放的分区扫描一遍输入 topic：
     * <ul>
     *   <li>可见记录没有映射 → 状态会漏掉这条输入；</li>
     *   <li>映射在窗口内但读不到记录 → 映射指向已删除的数据；</li>
     *   <li>可见记录数 != 映射数 → 窗口不完整。</li>
     * </ul>
     * 三者任一命中都中止恢复。代价是多读一遍尾部；可配置 strict-validation=false
     * 关闭，但不建议在生产恢复时关闭。
     */
    private void validateCoverage(RecoveryOffsetPlan plan, List<ReplayMapping> mappings) {
        // 只有 broker offset 领先快照的分区才需要重放和覆盖校验。
        Map<String, Map<Integer, ReplayInputFetcher.PartitionRange>> rangesByTopic =
                new LinkedHashMap<>();
        for (RecoveryOffsetPlan.PartitionPlan partition : plan.partitions()) {
            if (partition.brokerOffset() == null
                    || partition.brokerOffset() <= partition.snapshotOffset()) {
                continue;
            }
            rangesByTopic
                    .computeIfAbsent(partition.topic(), ignored -> new TreeMap<>())
                    .put(partition.partition(), new ReplayInputFetcher.PartitionRange(
                            partition.snapshotOffset(), partition.brokerOffset()));
        }

        // 期望集合直接从 offset-seq 映射派生；按分区排序后交给 fetcher 做二分查找，
        // 避免再构造一份 Set 放大内存。
        Map<String, Map<Integer, List<Long>>> expectedByTopic = new HashMap<>();
        for (ReplayMapping mapping : mappings) {
            expectedByTopic.computeIfAbsent(mapping.topic(), ignored -> new HashMap<>())
                    .computeIfAbsent(mapping.partition(), ignored -> new ArrayList<>())
                    .add(mapping.offset());
        }

        for (Map.Entry<String, Map<Integer, ReplayInputFetcher.PartitionRange>> entry
                : rangesByTopic.entrySet()) {
            Map<Integer, List<Long>> expected = expectedByTopic.getOrDefault(
                    entry.getKey(), Map.of());
            Map<Integer, long[]> expectedArrays = new HashMap<>(expected.size() * 2);
            expected.forEach((partition, offsets) -> {
                long[] values = offsets.stream().mapToLong(Long::longValue).sorted().toArray();
                expectedArrays.put(partition, values);
            });
            inputFetcher.validateCoverage(entry.getKey(), entry.getValue(), expectedArrays);
        }
    }

    /**
     * 把需要修正的消费组 offset 写回快照位点。
     *
     * <h2>为什么需要写 offset</h2>
     * 快照状态已经包含「快照切点之前处理过的所有记录」。如果 broker 上的 committed
     * offset C 缺失或落后于快照 offset S，正常 consumer 会从 C（或 earliest）重新消费，
     * 把快照已经包含的记录再执行一遍，直接造成重复扣款 / 重复冻结。因此这类分区必须
     * 把组位点显式抬到 S（{@link RecoveryOffsetPlan.Action#SET}）。
     *
     * <h2>为什么大多数情况不写</h2>
     * 正常崩溃场景是 C &gt; S：快照之后的记录已经处理并提交，状态重放又补齐了这段，
     * 所以 consumer 直接从 C 继续即可（{@link RecoveryOffsetPlan.Action#KEEP}）。此时
     * 不调用 {@code alterConsumerGroupOffsets}，避免启动期与旧组成员 / 组协调器交互，
     * 也避免任何不必要的位点移动。
     *
     * <h2>为什么放在重放之后</h2>
     * 只有状态重放成功才允许改 offset。否则会出现「位点已指向 S、内存却没恢复」的
     * 不一致状态。恢复失败时进程直接启动失败，不会带着被改过的 offset 对外服务。
     *
     * <h2>为什么按消费组分批</h2>
     * Admin API 以 group 为单位提交 offset。一个组通常对应一条输入链路（topic），
     * 这里按 group 聚合后一次提交，减少启动时的 admin 往返。
     *
     * <h2>GroupNotEmpty 重试</h2>
     * 进程刚崩溃重启时，旧进程的 consumer 成员可能还在 session timeout 内，
     * Kafka 不允许对非空组改 offset（GroupNotEmptyException）。默认 offset-timeout-ms
     * 60s 大于 session-timeout 45s，这里每隔 500ms 重试，等旧成员过期后完成写回。
     *
     * <p>本方法只处理 {@link RecoveryOffsetPlan#adjustments()}（Action=SET 的分区），
     * KEEP 分区不产生任何写操作。恢复本身是幂等的：重复执行会基于同样的 S/C 重新计算
     * 计划；第一次写回成功后，后续恢复会看到 C == S 而走 KEEP。
     *
     * @return 实际写过 offset 的消费组数量
     */
    private int applyOffsetAdjustments(RecoveryOffsetPlan plan) {
        // 只聚合 Action=SET 的分区；KEEP 分区保持 broker 位点不变。
        Map<String, Map<TopicPartition, OffsetAndMetadata>> byGroup = new LinkedHashMap<>();
        for (RecoveryOffsetPlan.PartitionPlan partition : plan.adjustments()) {
            byGroup.computeIfAbsent(partition.group(), ignored -> new LinkedHashMap<>())
                    .put(new TopicPartition(partition.topic(), partition.partition()),
                            new OffsetAndMetadata(partition.resumeOffset()));
        }
        if (byGroup.isEmpty()) {
            log.info("all consumer group offsets already match the snapshot; no adjustment needed");
            return 0;
        }

        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(properties.offsetTimeoutMs());
        for (Map.Entry<String, Map<TopicPartition, OffsetAndMetadata>> entry
                : byGroup.entrySet()) {
            String group = entry.getKey();
            while (true) {
                try {
                    adminClient.alterConsumerGroupOffsets(group, entry.getValue())
                            .all()
                            .get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
                    log.info("consumer group offsets reset to snapshot: group={}, partitions={}",
                            group, entry.getValue().size());
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SnapshotRecoveryException(
                            "interrupted while resetting offsets for group " + group);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof GroupNotEmptyException
                            && System.nanoTime() < deadline) {
                        // 旧进程的成员尚未过期；等 session timeout 后组变空再写。
                        log.info("consumer group {} still has active members; retrying offset "
                                + "reset", group);
                        sleep(OFFSET_RETRY_DELAY_MS);
                        continue;
                    }
                    throw new SnapshotRecoveryException(
                            "failed to reset offsets for group " + group + ": " + e.getCause(),
                            e);
                } catch (TimeoutException e) {
                    throw new SnapshotRecoveryException(
                            "timed out resetting offsets for group " + group, e);
                }
            }
        }
        return byGroup.size();
    }

    /**
     * 记录本次恢复的 offset 计划摘要。
     *
     * <p>{@code replayOffsetRange} 是各分区 [S, C) 的 offset 跨度之和，包含 aborted /
     * 不可见记录，因此它只是“扫描范围”的近似值；真正重放的记录数以
     * {@link ReplayApplier.ReplayStats} 为准。
     */
    private void logOffsetPlan(RecoveryOffsetPlan plan) {
        long replayPartitions = 0;
        long replayOffsetRange = 0;
        for (RecoveryOffsetPlan.PartitionPlan partition : plan.partitions()) {
            if (partition.brokerOffset() != null
                    && partition.brokerOffset() > partition.snapshotOffset()) {
                replayPartitions++;
                replayOffsetRange += partition.brokerOffset() - partition.snapshotOffset();
            }
        }
        log.info("recovery offset plan: partitions={}, replayPartitions={}, "
                        + "replayOffsetRange={}, adjustments={}",
                plan.partitions().size(), replayPartitions,
                replayOffsetRange, plan.adjustments().size());
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    /**
     * 建立 topic → 消费组的唯一映射。
     *
     * <p>offset-seq 映射只记录了输入 topic / 分区 / offset，没有 group；恢复需要用
     * (group, topic, partition) 定位 broker committed offset，因此这里要求同一条输入
     * topic 在快照中只属于一个消费组。出现多个组说明配置或快照拓扑有歧义，直接失败。
     */
    private Map<String, String> groupByTopic(List<TopicPartitionOffset> inputOffsets) {
        Map<String, String> groupByTopic = new HashMap<>();
        for (TopicPartitionOffset offset : inputOffsets) {
            String previous = groupByTopic.putIfAbsent(offset.topic(), offset.group());
            if (previous != null && !previous.equals(offset.group())) {
                throw new SnapshotRecoveryException("input topic " + offset.topic()
                        + " has multiple consumer groups in snapshot: " + previous + ", "
                        + offset.group());
            }
        }
        return groupByTopic;
    }

    private String topic(String key) {
        String topic = kafkaProperties.topics().get(key);
        if (topic == null || topic.isBlank()) {
            throw new SnapshotRecoveryException("missing app.kafka.topics." + key);
        }
        return topic;
    }

    private String group(String key) {
        String group = kafkaProperties.groups().get(key);
        if (group == null || group.isBlank()) {
            throw new SnapshotRecoveryException("missing app.kafka.groups." + key);
        }
        return group;
    }

    private static long remainingMillis(long deadlineNanos) {
        long remaining = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (remaining <= 0) {
            throw new SnapshotRecoveryException("snapshot offset reset timed out");
        }
        return remaining;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SnapshotRecoveryException(
                    "interrupted while waiting for consumer group members to expire");
        }
    }

    @Override
    public void stop() {
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        // phase 顺序：KafkaTopicInitializer(0) → 本恢复(10) → committer(50) → consumer(100)。
        // 重放使用 discard 输出，不依赖 committer；consumer 必须在恢复完成后才启动。
        return 10;
    }

    private record ExpectedTarget(String topic, String group) {
    }

    private record Selection(Path path, Long backupId, SnapshotBackupRecord record) {
    }

    private record MappingStats(Map<OffsetKey, Long> mappedCounts,
                                Map<OffsetKey, Long> maxMappedOffsets) {
    }
}
