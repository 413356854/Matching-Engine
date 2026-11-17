package com.me.order.service;

import com.me.kafka.BatchOutput;
import com.me.order.dto.OrderOperationResult;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCommandBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 委托操作执行器：三条委托输入链路（下单命令、冻结回执、撤单命令）的统一业务入口。
 *
 * <p>每个 {@code execute*} 方法在全局 {@code orderLock} 内完成一个消费批次：
 * <ol>
 *   <li>批次之间严格串行；批内记录按各链路原有的并发方式执行（下单逐条提交线程池、
 *       冻结回执并发更新 + 聚合撮合 + 并发清算、撤单并发查询 + 聚合撤销 + 并发更新）；</li>
 *   <li>处理完成后按 ConsumerRecord 输入顺序，为每条 Kafka 输入记录
 *       {@code orderSeq.incrementAndGet()} 分配独立序号，因此序号顺序就是输入记录被
 *       委托模块处理的顺序；</li>
 *   <li>在同一把 orderLock 内把冻结命令 / 撮合对 / 结算命令 / 委托历史和 offset-seq
 *       映射写入调用方传入的 {@link BatchOutput}，最终由 runner / committer 在同一个
 *       Kafka 事务里提交；</li>
 *   <li>返回 {@link OrderOperationResult}（输出内容的快照，供调用方检查 / 测试）：
 *       每条输入消费记录的 {@code (topic, partition, offset)} 与其独立序号、冻结命令 /
 *       撮合对 / 结算命令 / 委托历史；批次水位可由最后一条 offset 的序号推导。</li>
 * </ol>
 *
 * <p>Kafka 事务仍由 runner 处理：输出在 orderLock 内进入当前批次的 {@link BatchOutput}，
 * runner 打包后交给 committer，让「消费 offset + 输出 + 序列映射」原子提交。
 *
 * <p>批次串行只改变了跨链路的交错关系；批内并发与撮合引擎对象锁的语义保持不变。
 */
public interface OrderOperationService {

    /** 执行一个 {@code me.order.commands-batch} 下单批次。 */
    OrderOperationResult executeCommands(
            List<ConsumerRecord<String, OrderCommandBatch>> records, BatchOutput output);

    /** 执行一个 {@code me.order.freeze-results-batch} 冻结回执批次。 */
    OrderOperationResult executeFreezeResults(
            List<ConsumerRecord<String, OrderFreezeResultBatch>> records,
            BatchOutput output);

    /** 执行一个 {@code me.order.cancels-batch} 撤单批次。 */
    OrderOperationResult executeCancels(
            List<ConsumerRecord<String, OrderCancelBatch>> records, BatchOutput output);

    /**
     * 最近一次分配到的全局序号。
     *
     * <p>备份快照可以用它标记「已执行的委托操作水位」；读取时不会阻塞正在执行的批次，
     * 快照实现需要在委托全局锁内调用才能得到一致水位。
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
