package com.me.account.service.impl;

import com.me.account.dto.AccountApplyResult;
import com.me.account.dto.AccountOffsetSeq;
import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettleResult;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.dto.TradeSettlementCommand;
import com.me.account.service.AccountOperationService;
import com.me.account.service.AccountService;
import com.me.account.sharding.AccountSharding;
import com.me.kafka.KafkaSink;
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
 * 所有批次先竞争 {@code accountLock}，拿到锁后自增 {@link #accountSeq} 并把该序号
 * 写入本批每条输入的 offset-seq 映射。锁内再按 {@code slot(userId)} 把批内命令
 * 分成最多 {@link AccountSharding#SLOTS} 组提交共享线程池；同一 slot 的命令在组内
 * 按输入顺序串行，不同 slot 可以并行。因此不同批次之间严格串行，批内仍保留
 * 账户分片级的并发。
 *
 * <p><b>为什么整批加锁</b>：offset-seq 的价值在于「accountSeq 的顺序就是账户写入的
 * 实际顺序」。如果只锁单条命令，不同 consumer 的批次会交错执行，seq 无法用于快照恢复
 * 后的确定性重放。代价是账户写入的跨链路并发退化为「批内 slot 并发」，吞吐上限由
 * 单批大小和 slot 数决定。
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

    /** 账户操作的全局序号；每个批次在 {@link #accountLock} 内自增一次。 */
    private final AtomicLong accountSeq = new AtomicLong();

    /**
     * 批次级全局锁：充值 / 扣账、冻结、结算三条链路的批次在这里串行，
     * 保证 accountSeq 顺序与实际账户写入顺序一致。
     */
    private final ReentrantLock accountLock = new ReentrantLock();

    private final Map<RecordKey, AccountApplyResult> rejectedCommands = rejectedCache();
    private final Map<SettlementRecordKey, String> rejectedSettlements = rejectedCache();

    public AccountOperationServiceImpl(
            AccountService accountService,
            @Qualifier("accountOperationExecutor") ExecutorService slotExecutor) {
        this.accountService = accountService;
        this.slotExecutor = slotExecutor;
    }

    @Override
    public AccountOperationResult executeCommands(
            List<ConsumerRecord<String, FundsCommand>> records) {
        if (records == null || records.isEmpty()) {
            return AccountOperationResult.empty(accountSeq.get());
        }
        accountLock.lock();
        try {
            long seq = accountSeq.incrementAndGet();
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
            return new AccountOperationResult(seq, offsets(seq, records), flows, Map.of());
        } finally {
            accountLock.unlock();
        }
    }

    @Override
    public AccountOperationResult executeFreezeCommands(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records) {
        if (records == null || records.isEmpty()) {
            return AccountOperationResult.empty(accountSeq.get());
        }
        List<FreezeItem> items = flattenFreezeCommands(records);
        accountLock.lock();
        try {
            long seq = accountSeq.incrementAndGet();
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
            return new AccountOperationResult(seq, offsets(seq, records), flows, freezeResults);
        } finally {
            accountLock.unlock();
        }
    }

    @Override
    public AccountOperationResult executeSettlements(
            List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        if (records == null || records.isEmpty()) {
            return AccountOperationResult.empty(accountSeq.get());
        }
        List<SettleItem> items = flattenSettlements(records);
        accountLock.lock();
        try {
            long seq = accountSeq.incrementAndGet();
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
            return new AccountOperationResult(seq, offsets(seq, records), flows, Map.of());
        } finally {
            accountLock.unlock();
        }
    }

    @Override
    public long currentSeq() {
        return accountSeq.get();
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
        return key == null || key.isBlank() ? KafkaSink.BATCH_KEY : key;
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

    /** 本批每条输入消费记录与批次序号的映射；按输入顺序排列。 */
    private static List<AccountOffsetSeq> offsets(
            long seq, List<? extends ConsumerRecord<?, ?>> records) {
        List<AccountOffsetSeq> offsets = new ArrayList<>(records.size());
        for (ConsumerRecord<?, ?> record : records) {
            offsets.add(new AccountOffsetSeq(
                    record.topic(), record.partition(), record.offset(), seq));
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
