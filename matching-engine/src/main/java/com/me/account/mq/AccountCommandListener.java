package com.me.account.mq;

import com.me.account.dto.AccountApplyResult;
import com.me.account.dto.FundsCommand;
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
 * 消费 {@code me.account.commands}（key=userId）：充值、扣账等非冻结资金命令，
 * 更新内存余额；成功后发布资金流水。
 *
 * <p>下单冻结命令是独立链路：委托模块按 symbol 分组发往
 * {@code me.account.order-freeze-commands-batch}，由 {@link AccountFreezeCommandListener}
 * 消费。两条链路是不同的 consumer group，共用 {@link AccountService} 的分片锁，但拒绝缓存
 * 按各自的记录身份隔离。
 *
 * <p><b>批量消费</b>：{@code batch="true"} + {@code max.poll.records=2000}，一个 poll 批次
 * 在一个 Kafka 事务里提交「commands 消费 offset + 发往 funds 的消息」。发布失败会回滚整批
 * 并重投；演示版不做 requestId 防重，重投会再次执行。
 *
 * <p><b>拒绝结果缓存</b>：批量监听器在一条拒绝后仍会继续处理同批后续记录，后续记录可能改变余额；
 * 若整批回滚重投，直接重新执行可能把“先拒绝、后入账”重算成成功。这里按
 * {@code (partition, offset)} 缓存拒绝结果（LRU），重投时复现原拒绝。成功结果不缓存，
 * 重投会再次修改余额；后续由持久化备份/状态恢复解决。
 */
@Component
public class AccountCommandListener {

    private static final Logger log = LoggerFactory.getLogger(AccountCommandListener.class);

    /** 批量重投时复现拒绝结果的缓存上限；只保存拒绝结果，成功结果由 AccountService 缓存。 */
    static final int REJECTED_CACHE_LIMIT = 65_536;

    private final AccountService accountService;
    private final FundsFlowPublisher flowPublisher;

    private final Map<RecordKey, AccountApplyResult> rejected = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<RecordKey, AccountApplyResult> eldest) {
                    return size() > REJECTED_CACHE_LIMIT;
                }
            });

    public AccountCommandListener(AccountService accountService,
                                  FundsFlowPublisher flowPublisher) {
        this.accountService = accountService;
        this.flowPublisher = flowPublisher;
    }

    /**
     * 直连路径：同一线程内完成业务处理并发送输出（关闭 Disruptor 时使用）。
     */
    public void onCommand(List<ConsumerRecord<String, FundsCommand>> records) {
        publish(process(records));
    }

    /**
     * 纯业务处理：只更新余额并收集要发送的事件，不触碰 Kafka。
     *
     * <p>由 poll 线程调用；输出由 {@link #publish(AccountCommandBatch)} 在同一线程的
     * Kafka 事务内发送。
     */
    public AccountCommandBatch process(List<ConsumerRecord<String, FundsCommand>> records) {
        List<FundsFlowEvent> flows = new ArrayList<>(records.size());
        for (ConsumerRecord<String, FundsCommand> record : records) {
            handle(record.partition(), record.offset(), record.key(), record.value(), flows);
        }
        return new AccountCommandBatch(flows);
    }

    /**
     * 在当前 Kafka 事务（poll 线程）内发送本批输出。
     */
    public void publish(AccountCommandBatch batch) {
        if (!batch.flows().isEmpty()) {
            flowPublisher.publishBatch(batch.flows());
        }
    }

    /** 一批资金命令的处理结果：全部资金流水。 */
    public record AccountCommandBatch(List<FundsFlowEvent> flows) {
    }

    private void handle(int partition, long offset, String key, FundsCommand command,
                        List<FundsFlowEvent> flows) {
        RecordKey recordKey = new RecordKey(partition, offset);
        AccountApplyResult cachedRejection = rejected.get(recordKey);
        if (cachedRejection != null) {
            return;
        }

        AccountApplyResult result = accountService.apply(command);
        if (!result.success()) {
            rejected.put(recordKey, result);
            log.warn("account command rejected: partition={}, offset={}, key={}, "
                            + "reason={}, command={}",
                    partition, offset, key, result.reason(), command);
            return;
        }
        flows.add(result.flowEvent());
    }

    /** 消费记录身份；同一记录在整批回滚后重投时复用拒绝结果。 */
    private record RecordKey(int partition, long offset) {
    }
}
