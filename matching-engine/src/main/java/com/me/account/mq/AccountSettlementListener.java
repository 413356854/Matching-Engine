package com.me.account.mq;

import com.me.account.dto.TradeSettleResult;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.dto.TradeSettlementCommand;
import com.me.account.service.AccountService;
import com.me.account.sharding.AccountSharding;
import com.me.shared.event.FundsFlowEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.settlements-batch} 成交结算批量消息，对买方和卖方一次性结清。
 *
 * <p>消息按 userId slot 分区，key/partition=slot；一笔成交拆成买方、卖方两条单用户命令，
 * 账户模块按 userId slot 选择账户锁，不依赖跨用户原子性（金额已在下单冻结阶段扣除）。
 *
 * <p><b>批内并行</b>：把 poll 批次内的清算单按第一腿 userId 的 {@code slot} 分成最多 8 组，
 * 提交 {@code accountSettlementExecutor} 线程池并发结算；同一 slot 的命令在组内按输入顺序
 * 串行。全部完成后由 poll 线程按<b>原始命令顺序</b>汇合资金流水，再发送
 * {@code me.funds-batch}（{@code FundsFlowBatch}）。并行只发生在账户写入与流水构建，
 * 不改变流水的聚合顺序。
 *
 * <p>一条清算命令的全部资金腿由 {@link AccountService#settle} 作为一个原子操作执行；
 * 成功后每条腿产出一条资金流水。多用户命令（防御性支持）仍由 {@link AccountService}
 * 按升序锁定全部涉及的 slot，跨组并发不会破坏一致性。
 *
 * <p><b>批量消费</b>：一个 poll 批次一个 Kafka 事务；清算命令 offset 与
 * {@code me.funds-batch} 流水一起提交/回滚。结算被拒绝时等待其余分组完成后抛出异常，
 * 整批回滚并重投。演示版不缓存已成功的结算；重投时成功前缀会再次执行，可能失败或重复，
 * 后续由持久化备份与内存状态恢复解决。
 *
 * <p><b>拒绝结果缓存</b>：整批回滚后内存状态不回滚，其他消费线程（例如账户冻结命令）可能在同一
 * 用户/币种上改变冻结余额，使原本被拒的结算在重投时“变成功”。按批量消息内的
 * {@code (partition, offset, index)} 位置缓存拒绝原因，重投时复现同一拒绝，保持 fail-stop 的确定性。
 */
@Component
public class AccountSettlementListener {

    private static final Logger log = LoggerFactory.getLogger(AccountSettlementListener.class);

    /** 批量重投时复现拒绝结果的缓存上限。 */
    static final int REJECTED_CACHE_LIMIT = 65_536;

    private final AccountService accountService;
    private final FundsFlowPublisher flowPublisher;
    private final ExecutorService slotExecutor;

    private final Map<RecordKey, String> rejected = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<RecordKey, String> eldest) {
                    return size() > REJECTED_CACHE_LIMIT;
                }
            });

    public AccountSettlementListener(
            AccountService accountService,
            FundsFlowPublisher flowPublisher,
            @Qualifier("accountSettlementExecutor") ExecutorService slotExecutor) {
        this.accountService = accountService;
        this.flowPublisher = flowPublisher;
        this.slotExecutor = slotExecutor;
    }

    /** 直连路径：同一线程内完成业务处理并发送资金流水（关闭 Disruptor 时使用）。 */
    public void onSettlement(List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        publish(process(records));
    }

    /**
     * 纯业务处理：只更新余额并收集资金流水，不触碰 Kafka。
     *
     * <p>由 poll 线程调用；输出由
     * {@link #publish(AccountSettlementBatch)} 在 poll 线程的 Kafka 事务内发送。
     * 结算被拒绝时等待全部分组结束后抛出异常，供 runner 整批重投。
     */
    public AccountSettlementBatch process(
            List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        List<SettleItem> items = flatten(records);
        if (items.isEmpty()) {
            return new AccountSettlementBatch(List.of());
        }

        List<List<SettleItem>> bySlot = new ArrayList<>(AccountSharding.SLOTS);
        for (int slot = 0; slot < AccountSharding.SLOTS; slot++) {
            bySlot.add(new ArrayList<>());
        }
        for (SettleItem item : items) {
            bySlot.get(slotOf(item.command())).add(item);
        }

        @SuppressWarnings("unchecked")
        List<FundsFlowEvent>[] outcomes = new List[items.size()];
        List<Future<?>> futures = new ArrayList<>(AccountSharding.SLOTS);
        for (List<SettleItem> slotItems : bySlot) {
            if (slotItems.isEmpty()) {
                continue;
            }
            futures.add(slotExecutor.submit(() -> {
                for (SettleItem item : slotItems) {
                    outcomes[item.position()] = handle(item);
                }
                return null;
            }));
        }
        awaitAll(futures);

        List<FundsFlowEvent> flows = new ArrayList<>(items.size() * 4);
        for (int index = 0; index < outcomes.length; index++) {
            if (outcomes[index] == null) {
                throw new IllegalStateException(
                        "missing account settlement outcome at index " + index);
            }
            flows.addAll(outcomes[index]);
        }
        return new AccountSettlementBatch(flows);
    }

    /** 在当前 Kafka 事务（poll 线程）内发送本批资金流水。 */
    public void publish(AccountSettlementBatch batch) {
        if (!batch.flows().isEmpty()) {
            flowPublisher.publishBatch(batch.flows());
        }
    }

    /** 一批成交结算的处理结果：全部资金腿的流水。 */
    public record AccountSettlementBatch(List<FundsFlowEvent> flows) {
    }

    private static List<SettleItem> flatten(
            List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        List<SettleItem> items = new ArrayList<>();
        for (ConsumerRecord<String, TradeSettlementBatch> record : records) {
            TradeSettlementBatch message = record.value();
            if (message == null) {
                continue;
            }
            List<TradeSettlementCommand> commands = message.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                items.add(new SettleItem(items.size(), record, index, commands.get(index)));
            }
        }
        return items;
    }

    /**
     * 清算单归组用的 slot。正常命令只含一个用户；多用户命令按第一腿归组，
     * {@link AccountService#settle} 仍会按升序锁定全部涉及的 slot。
     */
    private static int slotOf(TradeSettlementCommand command) {
        if (command == null || command.legs().isEmpty()) {
            return 0;
        }
        return AccountSharding.slot(command.legs().get(0).userId());
    }

    private List<FundsFlowEvent> handle(SettleItem item) {
        ConsumerRecord<String, TradeSettlementBatch> record = item.record();
        RecordKey key = new RecordKey(record.topic(), record.partition(), record.offset(),
                item.index());
        String cachedReason = rejected.get(key);
        if (cachedReason != null) {
            log.error("trade settlement rejection replayed from batch cache: partition={}, "
                            + "offset={}, index={}, requestId={}, reason={}",
                    record.partition(), record.offset(), item.index(),
                    item.command() == null ? null : item.command().requestId(), cachedReason);
            throw new IllegalStateException("trade settlement rejected: " + cachedReason);
        }

        TradeSettleResult result = accountService.settle(item.command());
        if (!result.success()) {
            rejected.put(key, result.reason());
            log.error("trade settlement rejected, batch will retry: partition={}, offset={}, "
                            + "index={}, requestId={}, reason={}",
                    record.partition(), record.offset(), item.index(),
                    item.command() == null ? null : item.command().requestId(), result.reason());
            throw new IllegalStateException("trade settlement rejected: " + result.reason());
        }

        return result.flowEvents();
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
                        log.warn("additional account settlement worker failure", e.getCause());
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
                    "interrupted while waiting for account settlement workers");
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
        return new IllegalStateException("account settlement worker failed", cause);
    }

    /** 一条待处理清算命令及其在批量消息中的位置。 */
    private record SettleItem(
            int position,
            ConsumerRecord<String, TradeSettlementBatch> record,
            int index,
            TradeSettlementCommand command) {
    }

    /** 批量消息内的消费位置；整批回滚后重投时复用同一位置的拒绝原因。 */
    private record RecordKey(String topic, int partition, long offset, int index) {
    }
}
