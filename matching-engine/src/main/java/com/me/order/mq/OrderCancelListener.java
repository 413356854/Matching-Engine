package com.me.order.mq;

import com.me.order.dto.OrderOperationResult;
import com.me.order.service.OrderOperationService;
import com.me.shared.order.OrderCancelBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.order.cancels-batch}，发布解冻清算命令、委托历史与 offset-seq 映射。
 *
 * <p>业务执行在 {@link OrderOperationService}：批次在委托全局锁内串行并分配 orderSeq，
 * 批内仍按原有方式执行「并发查询 → 引擎对象锁内聚合撤销 → 并发更新委托」，最后汇总
 * 解冻清算命令与委托历史。
 *
 * <p>本类只做 Kafka 适配：所有输出与「输入 offset → orderSeq」映射在本 poll 线程的
 * 同一个 Kafka 事务内发送。
 *
 * <p>幂等：同一撤单命令重投时，委托已是 {@code CANCELED}，准备阶段返回
 * {@code ORDER_ALREADY_CANCELED}；更新阶段的 {@code computeIfPresent} 也会拒绝重复推进，
 * 不会重复解冻（与原有语义一致）。
 */
@Component
public class OrderCancelListener {

    private final OrderOperationService operationService;
    private final TradeSettlementCommandPublisher settlementPublisher;
    private final OrderHistoryPublisher orderHistoryPublisher;
    private final OrderOffsetSeqPublisher offsetSeqPublisher;

    public OrderCancelListener(OrderOperationService operationService,
                               TradeSettlementCommandPublisher settlementPublisher,
                               OrderHistoryPublisher orderHistoryPublisher,
                               OrderOffsetSeqPublisher offsetSeqPublisher) {
        this.operationService = operationService;
        this.settlementPublisher = settlementPublisher;
        this.orderHistoryPublisher = orderHistoryPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onCancel(List<ConsumerRecord<String, OrderCancelBatch>> records) {
        OrderOperationResult result = operationService.executeCancels(records);
        if (!result.settlementCommands().isEmpty()) {
            settlementPublisher.publishBatch(result.settlementCommands());
        }
        if (!result.historyEvents().isEmpty()) {
            orderHistoryPublisher.publishBatch(result.historyEvents());
        }
        offsetSeqPublisher.publish(result);
    }
}
