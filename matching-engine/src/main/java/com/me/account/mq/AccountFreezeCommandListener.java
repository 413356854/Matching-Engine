package com.me.account.mq;

import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.service.AccountOperationService;
import com.me.kafka.BatchOutput;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.account.order-freeze-commands-batch}，执行下单资金冻结。
 *
 * <p>topic 按 symbol 分区：同一 symbol 的冻结命令由单 consumer 顺序消费，冻结回执也按
 * symbol 汇总，撮合阶段的价格-时间优先不被破坏。
 *
 * <p>业务执行、accountSeq 分配以及资金流水 / 冻结回执 / offset-seq 的发布都在
 * {@link AccountOperationService} 的 accountLock 内完成；本类只做 Kafka 适配。
 */
@Component
public class AccountFreezeCommandListener {

    private final AccountOperationService operationService;

    public AccountFreezeCommandListener(AccountOperationService operationService) {
        this.operationService = operationService;
    }

    /** 消费按 symbol 分组的冻结命令批量消息。 */
    public void onFreezeCommandBatch(
            List<ConsumerRecord<String, OrderFreezeCommandsBatch>> records, BatchOutput output) {
        operationService.executeFreezeCommands(records, output);
    }
}
