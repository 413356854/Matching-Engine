package com.me.account.mq;

import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.service.AccountOperationService;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.order-freeze-commands-batch}，执行下单资金冻结。
 *
 * <p>topic 按 symbol 分区：同一 symbol 的冻结命令由单 consumer 顺序消费，冻结回执也按
 * symbol 汇总，撮合阶段的价格-时间优先不被破坏。
 *
 * <p>业务执行在 {@link AccountOperationService}：批次在账户全局锁内串行并分配
 * accountSeq，批内再按 {@code slot(userId)} 并发，同一用户的命令按输入顺序执行。
 * 本类只做 Kafka 适配：把资金流水、按 symbol 分组的冻结回执和「输入 offset →
 * accountSeq」映射在本 poll 线程的同一个 Kafka 事务内发送。
 */
@Component
public class AccountFreezeCommandListener {

    private final AccountOperationService operationService;
    private final FundsFlowPublisher flowPublisher;
    private final OrderFreezeResultPublisher freezeResultPublisher;
    private final AccountOffsetSeqPublisher offsetSeqPublisher;

    public AccountFreezeCommandListener(AccountOperationService operationService,
                                        FundsFlowPublisher flowPublisher,
                                        OrderFreezeResultPublisher freezeResultPublisher,
                                        AccountOffsetSeqPublisher offsetSeqPublisher) {
        this.operationService = operationService;
        this.flowPublisher = flowPublisher;
        this.freezeResultPublisher = freezeResultPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
    }

    /** 消费按 symbol 分组的冻结命令批量消息。 */
    public void onFreezeCommandBatch(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records) {
        AccountOperationResult result = operationService.executeFreezeCommands(records);
        if (!result.flows().isEmpty()) {
            flowPublisher.publishBatch(result.flows());
        }
        if (!result.freezeResults().isEmpty()) {
            freezeResultPublisher.publishBatch(result.freezeResults());
        }
        offsetSeqPublisher.publish(result);
    }
}
