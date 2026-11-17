package com.me.account.service.impl;

import com.me.account.dto.AccountApplyResult;
import com.me.account.dto.AccountOffsetSeq;
import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettleResult;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.dto.TradeSettlementCommand;
import com.me.account.mq.AccountOffsetSeqPublisher;
import com.me.account.mq.FundsFlowPublisher;
import com.me.account.mq.OrderFreezeResultPublisher;
import com.me.account.service.AccountBatchLock;
import com.me.account.service.AccountEntryLock;
import com.me.account.service.AccountOperationService;
import com.me.account.service.AccountService;
import com.me.account.sharding.AccountSharding;
import com.me.kafka.BatchOutput;
import com.me.kafka.FatalState;
import com.me.shared.event.FundsFlowEvent;
import com.me.shared.event.FundsLogType;
import com.me.shared.event.OrderFreezeResultEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import java.util.function.IntUnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * 账户操作执行器实现。
 *
 * <p><b>批次串行 + 批内按 slot 并发</b>：三条账户输入链路由不同 consumer 线程驱动，
 * 所有批次先竞争 {@code accountLock}。锁内按 {@code slot(userId)} 把批内命令分成最多
 * {@link AccountSharding#SLOTS} 组提交共享线程池并发处理；同一 slot 的命令在组内按输入
 * 顺序串行，不同 slot 可以并行。处理完成后，按 ConsumerRecord 输入顺序为每条输入记录
 * 自增 {@link #accountSeq} 并写入 offset-seq 映射；最后一条 offset 的序号即该批水位。
 * 因此不同批次之间严格串行，批内仍保留账户分片级的并发。
 *
 * <p><b>入口锁</b>：{@code me.account.commands} 入口批次在进入 {@code accountLock} 之前
 * 先获取 {@link AccountEntryLock}；冻结 / 结算内部链路不获取入口锁，因此快照等待内部
 * topic 排空时仍可继续。
 *
 * <p><b>为什么整批加锁</b>：offset-seq 的价值在于「accountSeq 的顺序就是输入记录被账户
 * 模块处理的顺序」；每条 Kafka 原生 offset 都有独立序号后，快照恢复可以精确定位每条输入。
 * 如果只锁单条命令，不同 consumer 的批次会交错执行，seq 无法用于快照恢复后的确定性重放。
 * 代价是账户写入的跨链路并发退化为「批内 slot 并发」，吞吐上限由单批大小和 slot 数决定。
 *
 * <p><b>输出发布也在 accountLock 内</b>：资金流水、冻结回执和 offset-seq 映射在
 * {@link #accountLock} 内把记录写入当前批次的 {@link BatchOutput}。
 * 这样「状态修改 → 分配 accountSeq → 发布输出」在同一个临界区内完成，listener 不再
 * 在 service 返回后再发布。Kafka 事务仍由 runner 统一提交。
 *
 * <p><b>拒绝缓存</b>：整批回滚后内存余额不回滚，重投时其他批次可能已经改变余额。
 * 这里按记录身份缓存拒绝结果（LRU），重投时复现同一拒绝；成功结果不缓存，重投会
 * 再次执行（演示版取舍）。命令按 {@code (topic, partition, offset)} 缓存，结算按
 * {@code (topic, partition, offset, index)} 缓存。
 */
@Service
public class AccountOperationServiceImpl implements AccountOperationService {

    private static final Logger log = LoggerFactory.getLogger(AccountOperationServiceImpl.class);

    /** 批量重投时复现拒绝结果的缓存上限；只保存拒绝结果。 */
    static final int REJECTED_CACHE_LIMIT = 65_536;

    private final AccountService accountService;
    private final ExecutorService slotExecutor;
    private final FundsFlowPublisher flowPublisher;
    private final OrderFreezeResultPublisher freezeResultPublisher;
    private final AccountOffsetSeqPublisher offsetSeqPublisher;
    private final FatalState fatalState;

    /** 账户操作的全局序号；每条 Kafka 输入记录在 {@link #accountLock} 内自增一次。 */
    private final AtomicLong accountSeq = new AtomicLong();

    /**
     * 批次级全局锁：充值 / 扣账、冻结、结算三条链路的批次在这里串行，
     * 保证 accountSeq 顺序与输入记录被账户模块处理的顺序一致。
     *
     * <p>runner 在 beginTransaction 前先获取同一把锁，并在 commit/abort 之后释放；
     * 本类内部再次获取可重入锁。因此本类释放后，runner 仍保持锁到 Kafka 事务结束，
     * 下一批不会在失败批次的脏状态上继续执行。
     */
    private final ReentrantLock accountLock;

    /**
     * 外部入口锁：{@code me.account.commands} 消费批次必须先取得，再进入
     * {@link #accountLock}。冻结 / 结算等内部链路不获取入口锁，快照持有入口锁等待
     * 排空时不会堵死内部级联。
     */
    private final ReentrantLock commandEntryLock;

    private final Map<RecordKey, AccountApplyResult> rejectedCommands = rejectedCache();
    private final Map<SettlementRecordKey, String> rejectedSettlements = rejectedCache();

    public AccountOperationServiceImpl(
            AccountService accountService,
            @Qualifier("accountOperationExecutor") ExecutorService slotExecutor,
            AccountBatchLock accountBatchLock,
            AccountEntryLock accountEntryLock,
            FundsFlowPublisher flowPublisher,
            OrderFreezeResultPublisher freezeResultPublisher,
            AccountOffsetSeqPublisher offsetSeqPublisher,
            FatalState fatalState) {
        this.accountService = accountService;
        this.slotExecutor = slotExecutor;
        this.accountLock = accountBatchLock.reentrantLock();
        this.commandEntryLock = accountEntryLock.reentrantLock();
        this.flowPublisher = flowPublisher;
        this.freezeResultPublisher = freezeResultPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
        this.fatalState = fatalState;
    }

    @Override
    public AccountOperationResult executeCommands(
            List<ConsumerRecord<String, FundsCommand>> records, BatchOutput output) {
        if (records == null || records.isEmpty()) {
            return AccountOperationResult.empty();
        }
        // 入口锁必须早于 accountLock：快照按「入口锁 → 模块锁」获取。
        commandEntryLock.lock();
        try {
            return executeCommandsLocked(records, output);
        } finally {
            commandEntryLock.unlock();
        }
    }

    private AccountOperationResult executeCommandsLocked(
            List<ConsumerRecord<String, FundsCommand>> records, BatchOutput output) {
        accountLock.lock();
        try {
            if (fatalState.isFatal()) {
                throw new IllegalStateException("[" + getClass().getSimpleName()
                        + "] engine is fatal; refusing batch");
            }
            FundsFlowEvent[] outcomes = new FundsFlowEvent[records.size()];
            executeBySlot(records.size(),
                    position -> slotOfCommand(records.get(position).value()),
                    position -> outcomes[position] = handleCommand(records.get(position)));

            List<FundsFlowEvent> flows = new ArrayList<>(records.size());
            for (FundsFlowEvent flow : outcomes) {
                if (flow != null) {
                    flows.add(flow);
                }
            }
            List<AccountOffsetSeq> offsets = offsets(records);
            AccountOperationResult result = new AccountOperationResult(offsets, flows, Map.of());
            publish(output, result);
            // 锁内直接路由到模块提交队列：BatchOutput.enqueue -> ModuleCommitPipeline.enqueue -> queue.put。
            output.enqueue();
            return result;
        } catch (RuntimeException e) {
            // 在释放锁之前进入全局 fatal，下一批拿到锁会看到 fatal 并退出。
            markFatal(output, e);
            throw e;
        } finally {
            accountLock.unlock();
        }
    }

    @Override
    public AccountOperationResult executeFreezeCommands(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records, BatchOutput output) {
        if (records == null || records.isEmpty()) {
            return AccountOperationResult.empty();
        }
        List<FreezeItem> items = flattenFreezeCommands(records);
        accountLock.lock();
        try {
            if (fatalState.isFatal()) {
                throw new IllegalStateException("[" + getClass().getSimpleName()
                        + "] engine is fatal; refusing batch");
            }
            FreezeOutcome[] outcomes = new FreezeOutcome[items.size()];
            executeBySlot(items.size(),
                    position -> slotOfCommand(items.get(position).command()),
                    position -> outcomes[position] = handleFreeze(items.get(position)));

            List<FundsFlowEvent> flows = new ArrayList<>(items.size());
            Map<String, List<OrderFreezeResultEvent>> freezeResults = new LinkedHashMap<>();
            for (FreezeOutcome outcome : outcomes) {
                if (outcome.flow() != null) {
                    flows.add(outcome.flow());
                }
                if (outcome.freezeResult() != null) {
                    freezeResults.computeIfAbsent(outcome.resultKey(),
                                    ignored -> new ArrayList<>())
                            .add(outcome.freezeResult());
                }
            }
            List<AccountOffsetSeq> offsets = offsets(records);
            AccountOperationResult result =
                    new AccountOperationResult(offsets, flows, freezeResults);
            publish(output, result);
            // 锁内直接路由到模块提交队列：BatchOutput.enqueue -> ModuleCommitPipeline.enqueue -> queue.put。
            output.enqueue();
            return result;
        } catch (RuntimeException e) {
            // 在释放锁之前进入全局 fatal，下一批拿到锁会看到 fatal 并退出。
            markFatal(output, e);
            throw e;
        } finally {
            accountLock.unlock();
        }
    }

    @Override
    public AccountOperationResult executeSettlements(
            List<ConsumerRecord<String, TradeSettlementBatch>> records, BatchOutput output) {
        if (records == null || records.isEmpty()) {
            return AccountOperationResult.empty();
        }
        List<SettleItem> items = flattenSettlements(records);
        accountLock.lock();
        try {
            if (fatalState.isFatal()) {
                throw new IllegalStateException("[" + getClass().getSimpleName()
                        + "] engine is fatal; refusing batch");
            }
            @SuppressWarnings("unchecked")
            List<FundsFlowEvent>[] outcomes = new List[items.size()];
            executeBySlot(items.size(),
                    position -> slotOfSettlement(items.get(position).command()),
                    position -> outcomes[position] = handleSettlement(items.get(position)));

            List<FundsFlowEvent> flows = new ArrayList<>(items.size() * 4);
            for (int index = 0; index < outcomes.length; index++) {
                if (outcomes[index] == null) {
                    throw new IllegalStateException(
                            "missing account settlement outcome at index " + index);
                }
                flows.addAll(outcomes[index]);
            }
            List<AccountOffsetSeq> offsets = offsets(records);
            AccountOperationResult result = new AccountOperationResult(offsets, flows, Map.of());
            publish(output, result);
            // 锁内直接路由到模块提交队列：BatchOutput.enqueue -> ModuleCommitPipeline.enqueue -> queue.put。
            output.enqueue();
            return result;
        } catch (RuntimeException e) {
            // 在释放锁之前进入全局 fatal，下一批拿到锁会看到 fatal 并退出。
            markFatal(output, e);
            throw e;
        } finally {
            accountLock.unlock();
        }
    }

    private void markFatal(BatchOutput output, RuntimeException error) {
        fatalState.markFatal(
                output == null ? getClass().getSimpleName() : output.source(), error);
    }

    /**
     * 在 accountLock 内发布本批输出：资金流水、冻结回执、offset-seq 映射。
     *
     * <p>publisher 直接把记录写入调用方传入的 {@link BatchOutput}，最终由 runner
     * 封装成 SendPackage、模块 committer 在同一个 Kafka 事务里提交。
     */
    private void publish(BatchOutput output, AccountOperationResult result) {
        if (!result.flows().isEmpty()) {
            flowPublisher.publishBatch(output, result.flows());
        }
        if (!result.freezeResults().isEmpty()) {
            freezeResultPublisher.publishBatch(output, result.freezeResults());
        }
        offsetSeqPublisher.publish(output, result);
    }

    @Override
    public long currentSeq() {
        return accountSeq.get();
    }

    @Override
    public void restoreSeq(long seq) {
        if (seq < 0) {
            throw new IllegalArgumentException("accountSeq must not be negative: " + seq);
        }
        long current = accountSeq.get();
        if (seq < current) {
            throw new IllegalStateException("accountSeq cannot move backwards: current="
                    + current + ", requested=" + seq);
        }
        accountSeq.set(seq);
    }

    /**
     * 按 slot 分组提交共享线程池；组内按输入顺序执行，全部结束后返回，失败时抛第一个异常。
     *
     * <p>必须先等全部分组结束再抛出失败：失败批次重投时不能让上一次的 worker 仍在修改账户。
     */
    private void executeBySlot(int itemCount, IntUnaryOperator slotOf, IntConsumer action) {
        if (itemCount == 0) {
            return;
        }
        List<List<Integer>> bySlot = new ArrayList<>(AccountSharding.SLOTS);
        for (int slot = 0; slot < AccountSharding.SLOTS; slot++) {
            bySlot.add(new ArrayList<>());
        }
        for (int position = 0; position < itemCount; position++) {
            bySlot.get(slotOf.applyAsInt(position)).add(position);
        }

        List<Future<?>> futures = new ArrayList<>(AccountSharding.SLOTS);
        for (List<Integer> slotItems : bySlot) {
            if (slotItems.isEmpty()) {
                continue;
            }
            futures.add(slotExecutor.submit(() -> {
                for (int position : slotItems) {
                    action.accept(position);
                }
                return null;
            }));
        }
        awaitAll(futures);
    }

    // ------------------------------------------------------------------
    // 充值 / 扣账
    // ------------------------------------------------------------------

    private FundsFlowEvent handleCommand(ConsumerRecord<String, FundsCommand> record) {
        RecordKey key = new RecordKey(record.topic(), record.partition(), record.offset());
        if (rejectedCommands.containsKey(key)) {
            return null;
        }

        AccountApplyResult result = accountService.apply(record.value());
        if (!result.success()) {
            rejectedCommands.put(key, result);
            log.warn("account command rejected: topic={}, partition={}, offset={}, key={}, "
                            + "reason={}, command={}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    result.reason(), record.value());
            return null;
        }
        return result.flowEvent();
    }

    // ------------------------------------------------------------------
    // 下单冻结
    // ------------------------------------------------------------------

    private static List<FreezeItem> flattenFreezeCommands(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records) {
        List<FreezeItem> items = new ArrayList<>();
        for (ConsumerRecord<String, OrderFreezeCommandsBatch> record : records) {
            OrderFreezeCommandsBatch message = record.value();
            if (message == null) {
                continue;
            }
            List<FundsCommand> commands = message.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                items.add(new FreezeItem(record, index, commands.get(index)));
            }
        }
        return items;
    }

    private FreezeOutcome handleFreeze(FreezeItem item) {
        String resultKey = freezeResultKey(item);
        FundsFlowEvent flow = null;
        OrderFreezeResultEvent freezeResult = null;
        FundsCommand command = item.command();
        if (command == null || command.logType() != FundsLogType.ORDER_FREEZE) {
            log.warn("non-freeze command on order-freeze topic ignored: partition={}, "
                            + "offset={}, index={}, key={}, command={}",
                    item.record().partition(), item.record().offset(), item.index(),
                    item.record().key(), command);
        } else {
            AccountApplyResult result = accountService.apply(command);
            if (!result.success()) {
                log.warn("account freeze rejected: partition={}, offset={}, index={}, "
                                + "key={}, reason={}, command={}",
                        item.record().partition(), item.record().offset(), item.index(),
                        item.record().key(), result.reason(), command);
                freezeResult = freezeResult(item, command, false, result.reason());
            } else {
                flow = result.flowEvent();
                freezeResult = freezeResult(item, command, true, null);
            }
        }
        return new FreezeOutcome(flow, freezeResult, resultKey);
    }

    private static OrderFreezeResultEvent freezeResult(FreezeItem item, FundsCommand command,
                                                       boolean success, String reason) {
        return new OrderFreezeResultEvent(
                command.requestId(),
                command.refId(),
                command.userId(),
                command.currency(),
                command.amount(),
                success,
                reason,
                Instant.now());
    }

    /**
     * 冻结回执的 symbol 分组：优先用命令携带的 symbol；旧消息退回输入 record key。
     */
    private static String freezeResultKey(FreezeItem item) {
        FundsCommand command = item.command();
        String symbol = command == null ? null : command.symbol();
        if (symbol != null && !symbol.isBlank()) {
            return symbol;
        }
        String key = item.record().key();
        return key == null || key.isBlank() ? BatchOutput.BATCH_KEY : key;
    }

    // ------------------------------------------------------------------
    // 成交结算
    // ------------------------------------------------------------------

    private static List<SettleItem> flattenSettlements(
            List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        List<SettleItem> items = new ArrayList<>();
        for (ConsumerRecord<String, TradeSettlementBatch> record : records) {
            TradeSettlementBatch message = record.value();
            if (message == null) {
                continue;
            }
            List<TradeSettlementCommand> commands = message.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                items.add(new SettleItem(record, index, commands.get(index)));
            }
        }
        return items;
    }

    private List<FundsFlowEvent> handleSettlement(SettleItem item) {
        ConsumerRecord<String, TradeSettlementBatch> record = item.record();
        SettlementRecordKey key = new SettlementRecordKey(record.topic(), record.partition(),
                record.offset(), item.index());
        String cachedReason = rejectedSettlements.get(key);
        if (cachedReason != null) {
            log.error("trade settlement rejection replayed from batch cache: partition={}, "
                            + "offset={}, index={}, requestId={}, reason={}",
                    record.partition(), record.offset(), item.index(),
                    item.command() == null ? null : item.command().requestId(), cachedReason);
            throw new IllegalStateException("trade settlement rejected: " + cachedReason);
        }

        TradeSettleResult result = accountService.settle(item.command());
        if (!result.success()) {
            rejectedSettlements.put(key, result.reason());
            log.error("trade settlement rejected, batch will retry: partition={}, offset={}, "
                            + "index={}, requestId={}, reason={}",
                    record.partition(), record.offset(), item.index(),
                    item.command() == null ? null : item.command().requestId(), result.reason());
            throw new IllegalStateException("trade settlement rejected: " + result.reason());
        }
        return result.flowEvents();
    }

    // ------------------------------------------------------------------
    // 公共辅助
    // ------------------------------------------------------------------

    /**
     * 本批每条 Kafka 输入记录与自己的全局 seq 的映射；按 ConsumerRecord 输入顺序，
     * 每条记录自增一次。
     *
     * <p>之前是“一个批次一个 seq，批内所有记录共用”；改为每条记录独立 seq 后，
     * 任意 Kafka 原生 offset 都能反查到唯一的 accountSeq。
     */
    private List<AccountOffsetSeq> offsets(List<? extends ConsumerRecord<?, ?>> records) {
        List<AccountOffsetSeq> offsets = new ArrayList<>(records.size());
        for (ConsumerRecord<?, ?> record : records) {
            offsets.add(new AccountOffsetSeq(
                    record.topic(), record.partition(), record.offset(),
                    accountSeq.incrementAndGet()));
        }
        return offsets;
    }

    private static int slotOfCommand(FundsCommand command) {
        return command == null ? 0 : AccountSharding.slot(command.userId());
    }

    /**
     * 清算单归组用的 slot。正常命令只含一个用户；多用户命令按第一腿归组，
     * {@link AccountService#settle} 仍会按升序锁定全部涉及的 slot。
     */
    private static int slotOfSettlement(TradeSettlementCommand command) {
        if (command == null || command.legs() == null || command.legs().isEmpty()) {
            return 0;
        }
        return AccountSharding.slot(command.legs().get(0).userId());
    }

    /**
     * 等待全部分组结束后再抛出第一个失败：避免失败批次重投时仍有工作线程修改账户。
     */
    private static void awaitAll(List<Future<?>> futures) {
        boolean interrupted = false;
        RuntimeException failure = null;
        for (Future<?> future : futures) {
            while (true) {
                try {
                    future.get();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    if (failure == null) {
                        failure = unwrap(e);
                    } else {
                        log.warn("additional account operation worker failure", e.getCause());
                    }
                    break;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (failure != null) {
            throw failure;
        }
        if (interrupted) {
            throw new IllegalStateException(
                    "interrupted while waiting for account operation workers");
        }
    }

    private static RuntimeException unwrap(ExecutionException e) {
        Throwable cause = e.getCause() == null ? e : e.getCause();
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("account operation worker failed", cause);
    }

    private static <K, V> Map<K, V> rejectedCache() {
        return Collections.synchronizedMap(new LinkedHashMap<>(256, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > REJECTED_CACHE_LIMIT;
            }
        });
    }

    /** 逐条资金命令的消费位置；整批回滚后重投时复用拒绝结果。 */
    private record RecordKey(String topic, int partition, long offset) {
    }

    /** 批量结算消息内的消费位置；整批回滚后重投时复用拒绝原因。 */
    private record SettlementRecordKey(String topic, int partition, long offset, int index) {
    }

    /** 一条待处理的冻结命令及其在批量消息中的位置。 */
    private record FreezeItem(
            ConsumerRecord<String, OrderFreezeCommandsBatch> record,
            int index,
            FundsCommand command) {
    }

    /** 单条冻结命令的处理结果。 */
    private record FreezeOutcome(FundsFlowEvent flow,
                                 OrderFreezeResultEvent freezeResult, String resultKey) {
    }

    /** 一条待处理的清算命令及其在批量消息中的位置。 */
    private record SettleItem(
            ConsumerRecord<String, TradeSettlementBatch> record,
            int index,
            TradeSettlementCommand command) {
    }
}
