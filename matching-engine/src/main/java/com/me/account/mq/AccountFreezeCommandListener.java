package com.me.account.mq;

import com.me.account.dto.AccountApplyResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.service.AccountService;
import com.me.account.sharding.AccountSharding;
import com.me.kafka.KafkaSink;
import com.me.shared.event.FundsFlowEvent;
import com.me.shared.event.FundsLogType;
import com.me.shared.event.OrderFreezeResultEvent;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.order-freeze-commands-batch}，执行下单资金冻结。
 *
 * <p>topic 仍按 symbol 分区：同一 symbol 的冻结命令由单 consumer 顺序消费，冻结回执也按
 * symbol 汇总，撮合阶段的价格-时间优先不被破坏。
 *
 * <p><b>批内并行</b>：consumer 拿到一个 symbol 批次后，按 {@code slot(userId)} 把命令拆成
 * 8 组，交给 8 个线程并发执行；同一用户的命令落在同一组内按原顺序处理。全部完成后由
 * consumer 线程按<b>原始命令顺序</b>汇合资金流水和冻结回执，再以一条 symbol 批量消息发往
 * {@code me.order.freeze-results-batch}。并行只发生在账户写入/流水构建，不改变回执顺序。
 *
 * <p><b>批量消费</b>：一个 poll 批次一个 Kafka 事务，冻结命令 offset、资金流水与冻结回执
 * 一起提交/回滚。发布失败整批回滚重投，由 {@link AccountService} 按 requestId 幂等去重。
 *
 * <p><b>拒绝结果缓存</b>：整批回滚后内存状态不回滚，而同批后续记录或并发线程可能改变余额；
 * 按批量消息内位置 {@code (partition, offset, index)} 缓存拒绝结果，重投时复现原拒绝，
 * 保持 fail-stop 的确定性。
 */
@Component
public class AccountFreezeCommandListener {

    private static final Logger log = LoggerFactory.getLogger(AccountFreezeCommandListener.class);

    /** 批量重投时复现拒绝结果的缓存上限。 */
    static final int REJECTED_CACHE_LIMIT = 65_536;

    private final AccountService accountService;
    private final FundsFlowPublisher flowPublisher;
    private final OrderFreezeResultPublisher freezeResultPublisher;

    /** 批内按 userId slot 并发的固定线程池；同时最多 8 个账户写线程。 */
    private final ExecutorService slotExecutor;

    private final Map<RecordKey, AccountApplyResult> rejected = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<RecordKey, AccountApplyResult> eldest) {
                    return size() > REJECTED_CACHE_LIMIT;
                }
            });

    public AccountFreezeCommandListener(AccountService accountService,
                                        FundsFlowPublisher flowPublisher,
                                        OrderFreezeResultPublisher freezeResultPublisher) {
        this.accountService = accountService;
        this.flowPublisher = flowPublisher;
        this.freezeResultPublisher = freezeResultPublisher;
        AtomicInteger threadIndex = new AtomicInteger();
        this.slotExecutor = Executors.newFixedThreadPool(AccountSharding.SLOTS, runnable -> {
            Thread thread = new Thread(runnable,
                    "account-freeze-slot-" + threadIndex.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    @PreDestroy
    public void shutdown() {
        slotExecutor.shutdownNow();
    }

    /** 消费按 symbol 分组的冻结命令批量消息，批内按 userId slot 并发冻结。 */
    public void onFreezeCommandBatch(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records) {
        List<FreezeItem> items = flatten(records);
        HandleOutcome[] outcomes = executeBySlot(items);

        List<FundsFlowEvent> flows = new ArrayList<>(items.size());
        Map<String, List<OrderFreezeResultEvent>> freezeResults = new LinkedHashMap<>();
        for (int i = 0; i < items.size(); i++) {
            HandleOutcome outcome = outcomes[i];
            if (outcome == null) {
                throw new IllegalStateException("missing account freeze outcome at index " + i);
            }
            if (outcome.flow() != null) {
                flows.add(outcome.flow());
            }
            if (outcome.freezeResult() != null) {
                freezeResults.computeIfAbsent(outcome.resultKey(), ignored -> new ArrayList<>())
                        .add(outcome.freezeResult());
            }
        }
        publish(flows, freezeResults);
    }

    private static List<FreezeItem> flatten(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records) {
        List<FreezeItem> items = new ArrayList<>();
        for (ConsumerRecord<String, OrderFreezeCommandsBatch> record : records) {
            OrderFreezeCommandsBatch message = record.value();
            if (message == null) {
                continue;
            }
            List<FundsCommand> commands = message.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                items.add(new FreezeItem(items.size(), record.partition(), record.offset(), index,
                        record.key(), commands.get(index)));
            }
        }
        return items;
    }

    /**
     * 按 userId slot 分组并发执行；outcomes 下标与输入命令位置一一对应，保证汇合顺序。
     */
    private HandleOutcome[] executeBySlot(List<FreezeItem> items) {
        HandleOutcome[] outcomes = new HandleOutcome[items.size()];
        if (items.isEmpty()) {
            return outcomes;
        }
        List<List<FreezeItem>> bySlot = new ArrayList<>(AccountSharding.SLOTS);
        for (int slot = 0; slot < AccountSharding.SLOTS; slot++) {
            bySlot.add(new ArrayList<>());
        }
        for (FreezeItem item : items) {
            int slot = item.command() == null
                    ? 0 : AccountSharding.slot(item.command().userId());
            bySlot.get(slot).add(item);
        }

        List<Future<?>> futures = new ArrayList<>(AccountSharding.SLOTS);
        for (List<FreezeItem> slotItems : bySlot) {
            if (slotItems.isEmpty()) {
                continue;
            }
            futures.add(slotExecutor.submit(() -> {
                for (FreezeItem item : slotItems) {
                    outcomes[item.position()] = handle(item);
                }
                return null;
            }));
        }

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while waiting for account freeze slot workers", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("account freeze slot worker failed", cause);
        }
        return outcomes;
    }

    private void publish(List<FundsFlowEvent> flows,
                         Map<String, List<OrderFreezeResultEvent>> freezeResults) {
        if (!flows.isEmpty()) {
            flowPublisher.publishBatch(flows);
        }
        if (!freezeResults.isEmpty()) {
            freezeResultPublisher.publishBatch(freezeResults);
        }
    }

    private HandleOutcome handle(FreezeItem item) {
        String resultKey = resultKey(item);
        FundsFlowEvent flow = null;
        OrderFreezeResultEvent freezeResult = null;
        FundsCommand command = item.command();
        if (command == null || command.logType() != FundsLogType.ORDER_FREEZE) {
            log.warn("non-freeze command on order-freeze topic ignored: partition={}, "
                            + "offset={}, index={}, key={}, command={}",
                    item.partition(), item.offset(), item.index(), item.groupKey(), command);
        } else {
            RecordKey recordKey = new RecordKey(item.partition(), item.offset(), item.index());
            AccountApplyResult cachedRejection = rejected.get(recordKey);
            if (cachedRejection != null) {
                freezeResult = freezeResult(item, command, false, cachedRejection.reason());
            } else {
                AccountApplyResult result = accountService.apply(command);
                if (!result.success()) {
                    rejected.put(recordKey, result);
                    log.warn("account freeze rejected: partition={}, offset={}, index={}, "
                                    + "key={}, reason={}, command={}",
                            item.partition(), item.offset(), item.index(), item.groupKey(),
                            result.reason(), command);
                    freezeResult = freezeResult(item, command, false, result.reason());
                } else {
                    flow = result.flowEvent();
                    freezeResult = freezeResult(item, command, true, null);
                }
            }
        }
        return new HandleOutcome(flow, freezeResult, resultKey);
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
    private static String resultKey(FreezeItem item) {
        FundsCommand command = item.command();
        String symbol = command == null ? null : command.symbol();
        if (symbol != null && !symbol.isBlank()) {
            return symbol;
        }
        String key = item.groupKey();
        return key == null || key.isBlank() ? KafkaSink.BATCH_KEY : key;
    }

    /**
     * 批量消息内的消费位置；整批回滚后重投时复用同一位置的拒绝原因。
     */
    private record RecordKey(int partition, long offset, int index) {
    }

    /** 一条待处理的冻结命令及其原始位置。 */
    private record FreezeItem(int position, int partition, long offset, int index,
                              String groupKey, FundsCommand command) {
    }

    /** 单条命令的处理结果；由 consumer 线程按原始顺序汇合。 */
    private record HandleOutcome(FundsFlowEvent flow,
                                 OrderFreezeResultEvent freezeResult, String resultKey) {
    }
}
