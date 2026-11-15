package com.me.account.mq;

import com.me.account.dto.AccountOperationResult;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.service.AccountOperationService;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.settlements-batch} 成交结算批量消息，对买方和卖方一次性结清。
 *
 * <p>消息按 userId slot 分区，key/partition=slot；一笔成交拆成买方、卖方两条单用户命令，
 * 账户模块按 userId slot 选择账户锁，不依赖跨用户原子性（金额已在下单冻结阶段扣除）。
 *
 * <p>业务执行在 {@link AccountOperationService}：批次在账户全局锁内串行并分配
 * accountSeq，批内再按 {@code slot(第一腿 userId)} 并发；同一 slot 的命令按输入顺序
 * 串行，资金流水按原始命令顺序汇合。结算被拒绝时等全部分组结束后抛出，runner 整批
 * 回滚重投，拒绝原因由服务按记录身份缓存并复现。
 *
 * <p>本类只做 Kafka 适配：把资金流水和「输入 offset → accountSeq」映射在本 poll 线程的
 * 同一个 Kafka 事务内发送。
 */
@Component
public class AccountSettlementListener {

    private final AccountOperationService operationService;
    private final FundsFlowPublisher flowPublisher;
    private final AccountOffsetSeqPublisher offsetSeqPublisher;

    public AccountSettlementListener(AccountOperationService operationService,
                                     FundsFlowPublisher flowPublisher,
                                     AccountOffsetSeqPublisher offsetSeqPublisher) {
        this.operationService = operationService;
        this.flowPublisher = flowPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
    }

    /** 消费清算批量消息；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onSettlement(List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        AccountOperationResult result = operationService.executeSettlements(records);
        if (!result.flows().isEmpty()) {
            flowPublisher.publishBatch(result.flows());
        }
        offsetSeqPublisher.publish(result);
    }
}
