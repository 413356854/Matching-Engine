package com.me.order.mq;

import com.me.kafka.BatchOutput;
import com.me.order.service.OrderOperationService;
import com.me.shared.event.OrderFreezeResultBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费账户模块的冻结回执批量消息，触发撮合与清算批次处理。
 *
 * <p>业务执行、orderSeq 分配以及撮合对 / 结算命令 / 委托历史 / offset-seq 的发布都在
 * {@link OrderOperationService} 的 orderLock 内完成；本类只做 Kafka 适配。
 *
 * <p>冻结回执按 symbol 分组、key=symbol，同一 symbol 的消息落在同一分区且保持生产顺序；
 * 多个 consumer 按分区分配，保证同一 symbol 只由一个 consumer 处理。
 */
@Component
public class OrderFreezeResultListener {

    private final OrderOperationService operationService;

    public OrderFreezeResultListener(OrderOperationService operationService) {
        this.operationService = operationService;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onFreezeResult(List<ConsumerRecord<String, OrderFreezeResultBatch>> records,
            BatchOutput output) {
        operationService.executeFreezeResults(records, output);
    }
}
