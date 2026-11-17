package com.me.account.mq;

import com.me.account.dto.FundsCommand;
import com.me.account.service.AccountOperationService;
import com.me.kafka.BatchOutput;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.commands}（key=userId）：充值、扣账等非冻结资金命令。
 *
 * <p>业务执行、accountSeq 分配以及资金流水 / offset-seq 的发布都在
 * {@link AccountOperationService} 的 accountLock 内完成；本类只做 Kafka 适配。
 *
 * <p>下单冻结命令是独立链路：委托模块按 symbol 分组发往
 * {@code me.account.order-freeze-commands-batch}，由 {@link AccountFreezeCommandListener}
 * 消费。两条链路是不同的 consumer group，但都经 {@link AccountOperationService} 的
 * 批次锁串行执行。
 */
@Component
public class AccountCommandListener {

    private final AccountOperationService operationService;

    public AccountCommandListener(AccountOperationService operationService) {
        this.operationService = operationService;
    }

    /** 消费一个 poll 批次；输出与消费 offset 由 runner 的事务统一提交。 */
    public void onCommand(List<ConsumerRecord<String, FundsCommand>> records,
                          BatchOutput output) {
        operationService.executeCommands(records, output);
    }
}
