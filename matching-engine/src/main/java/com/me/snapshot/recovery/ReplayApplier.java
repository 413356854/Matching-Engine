package com.me.snapshot.recovery;

import com.me.account.dto.AccountOffsetSeq;
import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.service.AccountOperationService;
import com.me.kafka.BatchOutput;
import com.me.kafka.KafkaClientProperties;
import com.me.order.dto.OrderOffsetSeq;
import com.me.order.dto.OrderOperationResult;
import com.me.order.service.OrderOperationService;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCommandBatch;
import com.me.snapshot.SnapshotRecoveryException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 按原始消费批次重放两个模块的输入记录。
 *
 * <p>重放复用正常 service 的状态机代码，输出写入 {@link BatchOutput#discard(String)}：
 * 这些批次在崩溃前已经提交过输出，不能重复发布。每批处理前把模块 seq 强制对齐到映射
 * 的首个 seq，处理后再逐条校验 service 新分配的 seq / 输入位置与 offset-seq 完全一致，
 * 任何偏差都说明快照或映射不可信，直接失败。
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class ReplayApplier {

    private static final Logger log = LoggerFactory.getLogger(ReplayApplier.class);

    private final OrderOperationService orderOperationService;
    private final AccountOperationService accountOperationService;
    private final ReplayInputFetcher fetcher;
    private final KafkaClientProperties kafkaProperties;
    private final BatchOutput discard = BatchOutput.discard("snapshot-replay");

    public ReplayApplier(OrderOperationService orderOperationService,
                         AccountOperationService accountOperationService,
                         ReplayInputFetcher fetcher,
                         KafkaClientProperties kafkaProperties) {
        this.orderOperationService = orderOperationService;
        this.accountOperationService = accountOperationService;
        this.fetcher = fetcher;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * 依次重放账户和委托模块的尾部批次。
     *
     * <p>两个模块的状态机彼此独立（通过 Kafka 异步解耦），因此重放顺序不影响最终状态：
     * 账户模块从自己的三条输入 topic 重建余额，委托模块从自己的三条输入 topic 重建委托和
     * 订单簿。账户先重放只是为了让余额尽早就绪，便于日志排查。
     *
     * <p>每个模块内部严格按 seq 升序；seq 是原运行期在模块锁内分配的全局处理顺序，
     * 跨 topic 的批次也按这个顺序交错重放，才能复现原始状态推进。
     */
    public ReplayStats replay(ReplayMappings mappings) {
        ModuleStats account = replayAccount(mappings.accountBatches());
        ModuleStats order = replayOrder(mappings.orderBatches());
        return new ReplayStats(order.batches(), order.records(),
                account.batches(), account.records());
    }

    /**
     * 重放委托模块的一个尾部批次。
     *
     * <p>步骤：
     * <ol>
     *   <li>把 {@code orderSeq} 对齐到本批第一条映射的 seq 前一位，让 service 重新分配出
     *       与原始运行完全相同的 seq。失败事务会留下 seq 空洞，因此允许向前跳；</li>
     *   <li>从 Kafka 原样抓取本批输入记录（保持映射中的顺序）；</li>
     *   <li>调用与正常消费完全相同的 service 方法，输出写入 discard；</li>
     *   <li>逐条比对 service 返回的 seq / topic / partition / offset 与映射。任何偏差
     *       都说明快照、映射或代码行为不一致，立即失败。</li>
     * </ol>
     */
    private ModuleStats replayOrder(List<ReplayBatch> batches) {
        if (batches.isEmpty()) {
            return new ModuleStats(0, 0);
        }
        long records = 0;
        String commands = topic("order-commands");
        String freezeResults = topic("order-freeze-results");
        String cancels = topic("order-cancels");
        for (ReplayBatch batch : batches) {
            long current = orderOperationService.currentSeq();
            if (batch.firstSeq() <= current) {
                throw new SnapshotRecoveryException("order replay batch seq " + batch.firstSeq()
                        + " is not after current orderSeq " + current);
            }
            orderOperationService.restoreSeq(batch.firstSeq() - 1);

            List<ConsumerRecord<String, ?>> fetched =
                    fetcher.fetch(batch.topic(), batch.mappings());
            OrderOperationResult result;
            if (batch.topic().equals(commands)) {
                result = orderOperationService.executeCommands(
                        cast(fetched), discard);
            } else if (batch.topic().equals(freezeResults)) {
                result = orderOperationService.executeFreezeResults(
                        cast(fetched), discard);
            } else if (batch.topic().equals(cancels)) {
                result = orderOperationService.executeCancels(
                        cast(fetched), discard);
            } else {
                throw new SnapshotRecoveryException(
                        "unsupported order replay input topic: " + batch.topic());
            }
            verifyOrder(batch, result.offsets());
            records += batch.mappings().size();
        }
        return new ModuleStats(batches.size(), records);
    }

    /**
     * 重放账户模块的一个尾部批次；步骤与 {@link #replayOrder(List)} 相同，只是把
     * {@code orderSeq} 换成 {@code accountSeq}，并调用账户 service。
     */
    private ModuleStats replayAccount(List<ReplayBatch> batches) {
        if (batches.isEmpty()) {
            return new ModuleStats(0, 0);
        }
        long records = 0;
        String commands = topic("account-commands");
        String freezeCommands = topic("account-order-freeze-commands");
        String settlements = topic("account-settlements");
        for (ReplayBatch batch : batches) {
            long current = accountOperationService.currentSeq();
            if (batch.firstSeq() <= current) {
                throw new SnapshotRecoveryException("account replay batch seq "
                        + batch.firstSeq() + " is not after current accountSeq " + current);
            }
            accountOperationService.restoreSeq(batch.firstSeq() - 1);

            List<ConsumerRecord<String, ?>> fetched =
                    fetcher.fetch(batch.topic(), batch.mappings());
            AccountOperationResult result;
            if (batch.topic().equals(commands)) {
                result = accountOperationService.executeCommands(
                        cast(fetched), discard);
            } else if (batch.topic().equals(freezeCommands)) {
                result = accountOperationService.executeFreezeCommands(
                        cast(fetched), discard);
            } else if (batch.topic().equals(settlements)) {
                result = accountOperationService.executeSettlements(
                        cast(fetched), discard);
            } else {
                throw new SnapshotRecoveryException(
                        "unsupported account replay input topic: " + batch.topic());
            }
            verifyAccount(batch, result.offsets());
            records += batch.mappings().size();
        }
        return new ModuleStats(batches.size(), records);
    }

    private static void verifyOrder(ReplayBatch batch, List<OrderOffsetSeq> actual) {
        if (actual == null || actual.size() != batch.mappings().size()) {
            throw new SnapshotRecoveryException("order replay seq mismatch for topic "
                    + batch.topic() + ": expected " + batch.mappings().size()
                    + " mappings, got " + (actual == null ? 0 : actual.size()));
        }
        for (int index = 0; index < actual.size(); index++) {
            ReplayMapping expected = batch.mappings().get(index);
            OrderOffsetSeq value = actual.get(index);
            if (value == null
                    || !expected.topic().equals(value.topic())
                    || expected.partition() != value.partition()
                    || expected.offset() != value.offset()
                    || expected.seq() != value.orderSeq()) {
                throw new SnapshotRecoveryException("order replay seq mismatch at index "
                        + index + " for topic " + batch.topic() + ": expected=" + expected
                        + ", actual=" + value);
            }
        }
    }

    private static void verifyAccount(ReplayBatch batch, List<AccountOffsetSeq> actual) {
        if (actual == null || actual.size() != batch.mappings().size()) {
            throw new SnapshotRecoveryException("account replay seq mismatch for topic "
                    + batch.topic() + ": expected " + batch.mappings().size()
                    + " mappings, got " + (actual == null ? 0 : actual.size()));
        }
        for (int index = 0; index < actual.size(); index++) {
            ReplayMapping expected = batch.mappings().get(index);
            AccountOffsetSeq value = actual.get(index);
            if (value == null
                    || !expected.topic().equals(value.topic())
                    || expected.partition() != value.partition()
                    || expected.offset() != value.offset()
                    || expected.seq() != value.accountSeq()) {
                throw new SnapshotRecoveryException("account replay seq mismatch at index "
                        + index + " for topic " + batch.topic() + ": expected=" + expected
                        + ", actual=" + value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> List<ConsumerRecord<String, T>> cast(
            List<ConsumerRecord<String, ?>> records) {
        return (List<ConsumerRecord<String, T>>) (List<?>) records;
    }

    private String topic(String key) {
        String topic = kafkaProperties.topics().get(key);
        if (topic == null || topic.isBlank()) {
            throw new SnapshotRecoveryException("missing app.kafka.topics." + key);
        }
        return topic;
    }

    public record ReplayStats(long orderBatches,
                              long orderRecords,
                              long accountBatches,
                              long accountRecords) {
    }

    private record ModuleStats(long batches, long records) {
    }
}
