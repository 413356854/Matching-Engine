package com.me.account.service;

import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettlementBatch;
import com.me.kafka.BatchOutput;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 账户操作执行器：三条账户输入链路（充值 / 扣账、下单冻结、成交结算）的统一业务入口。
 *
 * <p>每个 {@code execute*} 方法在账户全局锁内完成一个消费批次：
 * <ol>
 *   <li>批次之间严格串行；批内命令按 {@code slot(userId)} 分组提交共享线程池并发执行，
 *       同一 slot 内保持输入顺序；</li>
 *   <li>处理完成后按 ConsumerRecord 输入顺序，为每条 Kafka 输入记录
 *       {@code accountSeq.incrementAndGet()} 分配独立序号，因此序号顺序就是输入记录被
 *       账户模块处理的顺序；</li>
 *   <li>在同一把 accountLock 内把资金流水、冻结回执和 offset-seq 映射写入调用方传入的
 *       {@link BatchOutput}，最终由 runner / committer 在同一个 Kafka 事务里提交；</li>
 *   <li>返回 {@link AccountOperationResult}（输出内容的快照，供调用方检查 / 测试）：
 *       每条输入消费记录的 {@code (topic, partition, offset)} 与其独立序号、资金流水
 *       与（冻结链路的）回执；批次水位可由最后一条 offset 的序号推导。</li>
 * </ol>
 *
 * <p>Kafka 事务仍由 runner 处理：输出在 accountLock 内进入当前批次的 {@link BatchOutput}，
 * runner 打包后交给 committer，让「消费 offset + 输出 + 序列映射」原子提交。
 *
 * <p>批次串行不会破坏批内的多方并发：不同 slot 仍然并行，同一用户的命令仍按输入顺序
 * 执行。拒绝缓存也由本服务维护，保证整批回滚重投时复现原拒绝。
 */
public interface AccountOperationService {

    /** 执行一个 {@code me.account.commands} 批次（逐条 FundsCommand）。 */
    AccountOperationResult executeCommands(
            List<ConsumerRecord<String, FundsCommand>> records, BatchOutput output);

    /** 执行一个 {@code me.account.order-freeze-commands-batch} 冻结命令批次。 */
    AccountOperationResult executeFreezeCommands(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records,
            BatchOutput output);

    /** 执行一个 {@code me.account.settlements-batch} 成交结算批次。 */
    AccountOperationResult executeSettlements(
            List<ConsumerRecord<String, TradeSettlementBatch>> records,
            BatchOutput output);

    /**
     * 最近一次分配到的全局序号。
     *
     * <p>备份快照可以用它标记「已执行的账户操作水位」；读取时不会阻塞正在执行的批次，
     * 快照实现需要在账户全局锁内调用才能得到一致水位。
     */
    long currentSeq();

    /**
     * 恢复 / 重放用：把全局序号计数器设为指定值。
     *
     * <p>只能在启动恢复阶段、consumer 尚未启动时调用。允许向前跳（失败事务已分配但
     * 未提交的 seq 会形成空洞），不允许回退。
     */
    void restoreSeq(long seq);
}
