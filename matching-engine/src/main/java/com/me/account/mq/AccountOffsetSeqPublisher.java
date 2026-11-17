package com.me.account.mq;

import com.me.account.dto.AccountOffsetSeqBatch;
import com.me.account.dto.AccountOperationResult;
import com.me.kafka.BatchOutput;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把「账户输入消费记录 → accountSeq」的映射发往 {@code me.account.offset-seq-batch}。
 *
 * <p>{@link com.me.account.service.AccountOperationService} 按输入顺序为每条 Kafka
 * 输入记录分配独立序号，并把（topic / partition / offset）与该序号配对；这里把整批映射
 * 封装成<b>一条</b>批量消息（固定 key {@code batch}，落同一分区），与资金流水、冻结回执、
 * 消费 offset 在同一个 Kafka 事务内提交。
 *
 * <p>备份快照恢复后回溯 Kafka 消息时，按 {@code accountSeq} 顺序重放输入消息，
 * 并用快照水位过滤已经执行过的记录。
 */
@Component
public class AccountOffsetSeqPublisher {

    private final String topic;

    public AccountOffsetSeqPublisher(
            @Value("${app.kafka.topics.account-offset-seq}") String topic) {
        this.topic = topic;
    }

    /** 把本批的 offset-seq 映射放入本批次输出；没有输入记录时跳过。 */
    public void publish(BatchOutput output, AccountOperationResult result) {
        if (result.offsets().isEmpty()) {
            return;
        }
        output.sendBatchMessage(topic, result.offsets(), AccountOffsetSeqBatch::new);
    }
}
