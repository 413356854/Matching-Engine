package com.me.account.mq;

import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.FundsCommand;
import com.me.account.service.AccountOperationService;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.commands}（key=userId）：充值、扣账等非冻结资金命令，
 * 成功后发布资金流水与 offset-seq 映射。
 *
 * <p>业务执行在 {@link AccountOperationService}：批次在账户全局锁内串行并分配
 * accountSeq，批内再按 {@code slot(userId)} 并发。本类只做 Kafka 适配：把结果中的
 * 资金流水和「输入 offset → accountSeq」映射在本 poll 线程的同一个 Kafka 事务内发送。
 *
 * <p>下单冻结命令是独立链路：委托模块按 symbol 分组发往
 * {@code me.account.order-freeze-commands-batch}，由 {@link AccountFreezeCommandListener}
 * 消费。两条链路是不同的 consumer group，但都经 {@link AccountOperationService} 的
 * 批次锁串行执行。
 */
@Component
public class AccountCommandListener {

    private final AccountOperationService operationService;
    private final FundsFlowPublisher flowPublisher;
    private final AccountOffsetSeqPublisher offsetSeqPublisher;

    public AccountCommandListener(AccountOperationService operationService,
                                  FundsFlowPublisher flowPublisher,
                                  AccountOffsetSeqPublisher offsetSeqPublisher) {
        this.operationService = operationService;
        this.flowPublisher = flowPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onCommand(List<ConsumerRecord<String, FundsCommand>> records) {
        AccountOperationResult result = operationService.executeCommands(records);
        if (!result.flows().isEmpty()) {
            flowPublisher.publishBatch(result.flows());
        }
        offsetSeqPublisher.publish(result);
    }
}
