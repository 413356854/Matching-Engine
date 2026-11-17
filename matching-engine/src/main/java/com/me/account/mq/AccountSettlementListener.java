package com.me.account.mq;

import com.me.account.dto.TradeSettlementBatch;
import com.me.account.service.AccountOperationService;
import com.me.kafka.BatchOutput;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.settlements-batch} 成交结算批量消息，对买方和卖方一次性结清。
 *
 * <p>消息按 userId slot 分区，key/partition=slot；一笔成交拆成买方、卖方两条单用户命令，
 * 账户模块按 userId slot 选择账户锁，不依赖跨用户原子性（金额已在下单冻结阶段扣除）。
 *
 * <p>业务执行、accountSeq 分配以及资金流水 / offset-seq 的发布都在
 * {@link AccountOperationService} 的 accountLock 内完成；本类只做 Kafka 适配。
 */
@Component
public class AccountSettlementListener {

    private final AccountOperationService operationService;

    public AccountSettlementListener(AccountOperationService operationService) {
        this.operationService = operationService;
    }

    /** 消费清算批量消息；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onSettlement(List<ConsumerRecord<String, TradeSettlementBatch>> records,
                             BatchOutput output) {
        operationService.executeSettlements(records, output);
    }
}
