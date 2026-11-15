package com.me.order.mq;

import com.me.order.dto.OrderOperationResult;
import com.me.order.service.OrderOperationService;
import com.me.shared.order.OrderCommandBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费委托命令，发布冻结命令与 offset-seq 映射。
 *
 * <p><b>唯一接单入口</b>：币币模块的 {@code POST /api/v1/currency/orders} 只向
 * {@code me.order.commands-batch} 投递命令，不直接调用委托模块；本监听器消费该 topic，
 * 是系统唯一的下单方式与入口。
 *
 * <p>业务执行在 {@link OrderOperationService}：批次在委托全局锁内串行并分配
 * orderSeq，批内命令仍按原方式提交线程池并发保存、按原命令顺序收集冻结命令。
 * 本类只做 Kafka 适配：把冻结命令与「输入 offset → orderSeq」映射在本 poll 线程的
 * 同一个 Kafka 事务内发送。
 *
 * <p>冻结命令按 symbol 分组后发往 {@code me.account.order-freeze-commands-batch}，key=symbol；
 * 充值等其它资金命令仍在 {@code me.account.commands}（key=userId）。
 */
@Component
public class OrderCommandListener {

    private final OrderOperationService operationService;
    private final OrderFreezeCommandPublisher freezeCommandPublisher;
    private final OrderOffsetSeqPublisher offsetSeqPublisher;

    public OrderCommandListener(OrderOperationService operationService,
                                OrderFreezeCommandPublisher freezeCommandPublisher,
                                OrderOffsetSeqPublisher offsetSeqPublisher) {
        this.operationService = operationService;
        this.freezeCommandPublisher = freezeCommandPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onCommand(List<ConsumerRecord<String, OrderCommandBatch>> records) {
        OrderOperationResult result = operationService.executeCommands(records);
        if (!result.freezeCommands().isEmpty()) {
            freezeCommandPublisher.publishBatch(result.freezeCommands());
        }
        offsetSeqPublisher.publish(result);
    }
}
