package com.me.order.service;

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
 *   <li>先 {@code orderSeq.incrementAndGet()} 分配本批次的全局序号，批次之间严格串行，
 *       因此序号顺序就是委托状态推进的执行顺序；</li>
 *   <li>批内记录按各链路原有的并发方式执行（下单逐条提交线程池、冻结回执并发更新 +
 *       聚合撮合 + 并发清算、撤单并发查询 + 聚合撤销 + 并发更新）；</li>
 *   <li>返回 {@link OrderOperationResult}：批次序号、每条输入消费记录的
 *       {@code (topic, partition, offset)}、以及冻结命令 / 撮合对 / 结算命令 / 委托历史。</li>
 * </ol>
 *
 * <p>Kafka 事务不在这里处理：listener 拿到结果后，需要在当前 poll 线程绑定的同一事务里
 * 发送各链路输出和 offset-seq 映射，让「消费 offset + 输出 + 序列映射」原子提交。
 *
 * <p>批次串行只改变了跨链路的交错关系；批内并发与撮合引擎对象锁的语义保持不变。
 */
public interface OrderOperationService {

    /** 执行一个 {@code me.order.commands-batch} 下单批次。 */
    OrderOperationResult executeCommands(
            List<ConsumerRecord<String, OrderCommandBatch>> records);

    /** 执行一个 {@code me.order.freeze-results-batch} 冻结回执批次。 */
    OrderOperationResult executeFreezeResults(
            List<ConsumerRecord<String, OrderFreezeResultBatch>> records);

    /** 执行一个 {@code me.order.cancels-batch} 撤单批次。 */
    OrderOperationResult executeCancels(
            List<ConsumerRecord<String, OrderCancelBatch>> records);

    /**
     * 最近一次分配到的全局序号。
     *
     * <p>备份快照可以用它标记「已执行的委托操作水位」；读取时不会阻塞正在执行的批次，
     * 快照实现需要在委托全局锁内调用才能得到一致水位。
     */
    long currentSeq();
}
