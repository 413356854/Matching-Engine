package com.me.account.mq;

import com.me.account.dto.TradeSettleResult;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.dto.TradeSettlementCommand;
import com.me.account.service.AccountService;
import com.me.shared.event.FundsFlowEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.settlements-batch} 双边成交结算批量消息，对买方和卖方一次性结清。
 * 消息按 userId slot 分组，key/partition=slot；一笔成交拆成买方、卖方两条单用户命令，
 * 账户模块按 userId 选择账户锁，不依赖跨用户原子性（金额已在下单冻结阶段扣除）。
 *
 * <p>一条消息包含一个成交对的全部资金腿（买方扣计价币 / 收到基础币，卖方扣基础币 / 收到计价币），
 * {@link AccountService#settle} 把全部腿作为一个原子操作执行；成功后每条腿产出一条资金流水。
 *
 * <p><b>批量消费</b>：{@code batch="true"}，一个 poll 批次一个 Kafka 事务；每条
 * {@link TradeSettlementBatch} 消息内部再按列表顺序逐条结算。结算被拒绝时立即抛异常，
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

    private final Map<RecordKey, String> rejected = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<RecordKey, String> eldest) {
                    return size() > REJECTED_CACHE_LIMIT;
                }
            });

    public AccountSettlementListener(AccountService accountService,
                                     FundsFlowPublisher flowPublisher) {
        this.accountService = accountService;
        this.flowPublisher = flowPublisher;
    }

    /** 直连路径：同一线程内完成业务处理并发送资金流水（关闭 Disruptor 时使用）。 */
    public void onSettlement(List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        publish(process(records));
    }

    /**
     * 纯业务处理：只更新余额并收集资金流水，不触碰 Kafka。
     *
     * <p>由 account ingress handler 线程调用；输出由 {@link #publish(AccountSettlementBatch)}
     * 在 poll 线程的 Kafka 事务内发送。结算被拒绝时抛出异常，供 runner 整批重投。
     */
    public AccountSettlementBatch process(
            List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        List<FundsFlowEvent> flows = new ArrayList<>(records.size() * 4);
        for (ConsumerRecord<String, TradeSettlementBatch> record : records) {
            TradeSettlementBatch message = record.value();
            if (message == null) {
                continue;
            }
            List<TradeSettlementCommand> commands = message.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                flows.addAll(handle(record, index, commands.get(index)));
            }
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

    private List<FundsFlowEvent> handle(ConsumerRecord<String, TradeSettlementBatch> record,
                                        int index,
                                        TradeSettlementCommand command) {
        RecordKey key = new RecordKey(record.topic(), record.partition(), record.offset(), index);
        String cachedReason = rejected.get(key);
        if (cachedReason != null) {
            log.error("trade settlement rejection replayed from batch cache: partition={}, "
                            + "offset={}, index={}, requestId={}, reason={}",
                    record.partition(), record.offset(), index,
                    command == null ? null : command.requestId(), cachedReason);
            throw new IllegalStateException("trade settlement rejected: " + cachedReason);
        }

        TradeSettleResult result = accountService.settle(command);
        if (!result.success()) {
            rejected.put(key, result.reason());
            log.error("trade settlement rejected, batch will retry: partition={}, offset={}, "
                            + "index={}, requestId={}, reason={}",
                    record.partition(), record.offset(), index,
                    command == null ? null : command.requestId(), result.reason());
            throw new IllegalStateException("trade settlement rejected: " + result.reason());
        }

        return result.flowEvents();
    }

    /** 批量消息内的消费位置；整批回滚后重投时复用同一位置的拒绝原因。 */
    private record RecordKey(String topic, int partition, long offset, int index) {
    }
}
