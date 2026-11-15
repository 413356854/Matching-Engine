package com.me.account.service;

import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettlementBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 账户操作执行器：三条账户输入链路（充值 / 扣账、下单冻结、成交结算）的统一业务入口。
 *
 * <p>每个 {@code execute*} 方法在账户全局锁内完成一个消费批次：
 * <ol>
 *   <li>先 {@code accountSeq.incrementAndGet()} 分配本批次的全局序号，批次之间严格串行，
 *       因此序号顺序就是账户写入的执行顺序；</li>
 *   <li>批内命令再按 {@code slot(userId)} 分组提交共享线程池并发执行，同一 slot 内保持
 *       输入顺序；</li>
 *   <li>返回 {@link AccountOperationResult}：批次序号、每条输入消费记录的
 *       {@code (topic, partition, offset)}、资金流水与（冻结链路的）回执。</li>
 * </ol>
 *
 * <p>Kafka 事务不在这里处理：listener 拿到结果后，需要在当前 poll 线程绑定的同一事务里
 * 发送资金流水、冻结回执和 offset-seq 映射，让「消费 offset + 输出 + 序列映射」原子提交。
 *
 * <p>批次串行不会破坏批内的多方并发：不同 slot 仍然并行，同一用户的命令仍按输入顺序
 * 执行。拒绝缓存也由本服务维护，保证整批回滚重投时复现原拒绝。
 */
public interface AccountOperationService {

    /** 执行一个 {@code me.account.commands} 批次（逐条 FundsCommand）。 */
    AccountOperationResult executeCommands(
            List<ConsumerRecord<String, FundsCommand>> records);

    /** 执行一个 {@code me.account.order-freeze-commands-batch} 冻结命令批次。 */
    AccountOperationResult executeFreezeCommands(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records);

    /** 执行一个 {@code me.account.settlements-batch} 成交结算批次。 */
    AccountOperationResult executeSettlements(
            List<ConsumerRecord<String, TradeSettlementBatch>> records);

    /**
     * 最近一次分配到的全局序号。
     *
     * <p>备份快照可以用它标记「已执行的账户操作水位」；读取时不会阻塞正在执行的批次，
     * 快照实现需要在账户全局锁内调用才能得到一致水位。
     */
    long currentSeq();
}
