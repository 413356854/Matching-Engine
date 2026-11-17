package com.me.order.mq;

import com.me.kafka.BatchOutput;
import com.me.order.service.OrderOperationService;
import com.me.shared.order.OrderCancelBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.order.cancels-batch}，触发撤单批次处理。
 *
 * <p>业务执行、orderSeq 分配以及解冻清算命令 / 委托历史 / offset-seq 的发布都在
 * {@link OrderOperationService} 的 orderLock 内完成；本类只做 Kafka 适配。
 *
 * <p>幂等：同一撤单命令重投时，委托已是 {@code CANCELED}，准备阶段返回
 * {@code ORDER_ALREADY_CANCELED}；更新阶段的 {@code computeIfPresent} 也会拒绝重复推进，
 * 不会重复解冻（与原有语义一致）。
 */
@Component
public class OrderCancelListener {

    private final OrderOperationService operationService;

    public OrderCancelListener(OrderOperationService operationService) {
        this.operationService = operationService;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onCancel(List<ConsumerRecord<String, OrderCancelBatch>> records,
            BatchOutput output) {
        operationService.executeCancels(records, output);
    }
}
