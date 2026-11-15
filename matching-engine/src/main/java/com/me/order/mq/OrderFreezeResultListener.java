package com.me.order.mq;

import com.me.order.dto.OrderOperationResult;
import com.me.order.service.OrderOperationService;
import com.me.shared.event.OrderFreezeResultBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费账户模块的冻结回执批量消息，发布撮合对、结算命令、委托历史与 offset-seq 映射。
 *
 * <p>业务执行在 {@link OrderOperationService}：批次在委托全局锁内串行并分配 orderSeq，
 * 批内仍按原有方式执行「并发更新订单 → 引擎对象锁内聚合撮合 → 并发成交清算」，
 * 最后按 symbol 聚合撮合对、汇总结算命令与委托历史。
 *
 * <p>本类只做 Kafka 适配：所有输出与「输入 offset → orderSeq」映射在本 poll 线程的
 * 同一个 Kafka 事务内发送。
 *
 * <p>冻结回执按 symbol 分组、key=symbol，同一 symbol 的消息落在同一分区且保持生产顺序；
 * 多个 consumer 按分区分配，保证同一 symbol 只由一个 consumer 处理。
 */
@Component
public class OrderFreezeResultListener {

    private final OrderOperationService operationService;
    private final MatchPairEventPublisher matchPairPublisher;
    private final TradeSettlementCommandPublisher settlementPublisher;
    private final OrderHistoryPublisher orderHistoryPublisher;
    private final OrderOffsetSeqPublisher offsetSeqPublisher;

    public OrderFreezeResultListener(
            OrderOperationService operationService,
            MatchPairEventPublisher matchPairPublisher,
            TradeSettlementCommandPublisher settlementPublisher,
            OrderHistoryPublisher orderHistoryPublisher,
            OrderOffsetSeqPublisher offsetSeqPublisher) {
        this.operationService = operationService;
        this.matchPairPublisher = matchPairPublisher;
        this.settlementPublisher = settlementPublisher;
        this.orderHistoryPublisher = orderHistoryPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onFreezeResult(List<ConsumerRecord<String, OrderFreezeResultBatch>> records) {
        OrderOperationResult result = operationService.executeFreezeResults(records);
        if (!result.matchPairsBySymbol().isEmpty()) {
            matchPairPublisher.publishBatch(result.matchPairsBySymbol());
        }
        if (!result.settlementCommands().isEmpty()) {
            settlementPublisher.publishBatch(result.settlementCommands());
        }
        if (!result.historyEvents().isEmpty()) {
            orderHistoryPublisher.publishBatch(result.historyEvents());
        }
        offsetSeqPublisher.publish(result);
    }
}
