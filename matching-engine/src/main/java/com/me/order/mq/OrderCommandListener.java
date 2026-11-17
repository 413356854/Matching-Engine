package com.me.order.mq;

import com.me.kafka.BatchOutput;
import com.me.order.service.OrderOperationService;
import com.me.shared.order.OrderCommandBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费委托命令，触发下单批次处理。
 *
 * <p><b>唯一接单入口</b>：币币模块的 {@code POST /api/v1/currency/orders} 只向
 * {@code me.order.commands-batch} 投递命令，不直接调用委托模块；本监听器消费该 topic，
 * 是系统唯一的下单方式与入口。
 *
 * <p>业务执行、orderSeq 分配以及冻结命令 / offset-seq 的发布都在
 * {@link OrderOperationService} 的 orderLock 内完成；本类只做 Kafka 适配。
 *
 * <p>冻结命令按 symbol 分组后发往 {@code me.account.order-freeze-commands-batch}，key=symbol；
 * 充值等其它资金命令仍在 {@code me.account.commands}（key=userId）。
 */
@Component
public class OrderCommandListener {

    private final OrderOperationService operationService;

    public OrderCommandListener(OrderOperationService operationService) {
        this.operationService = operationService;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onCommand(List<ConsumerRecord<String, OrderCommandBatch>> records,
            BatchOutput output) {
        operationService.executeCommands(records, output);
    }
}
