package com.me.order.mq;

import com.me.kafka.BatchOutput;
import com.me.order.dto.OrderOffsetSeqBatch;
import com.me.order.dto.OrderOperationResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把「委托输入消费记录 → orderSeq」的映射发往 {@code me.order.offset-seq-batch}。
 *
 * <p>{@link com.me.order.service.OrderOperationService} 按输入顺序为每条 Kafka 输入记录
 * 分配独立序号，并把（topic / partition / offset）与该序号配对；这里把整批映射封装成
 * <b>一条</b>批量消息（固定 key {@code batch}，落同一分区），与撮合输出、结算命令、
 * 委托历史及消费 offset 在同一个 Kafka 事务内提交。
 *
 * <p>备份快照恢复后回溯 Kafka 消息时，按 {@code orderSeq} 顺序重放输入消息，
 * 并用快照水位过滤已经执行过的记录。
 */
@Component
public class OrderOffsetSeqPublisher {

    private final String topic;

    public OrderOffsetSeqPublisher(
            @Value("${app.kafka.topics.order-offset-seq}") String topic) {
        this.topic = topic;
    }

    /** 把本批的 offset-seq 映射封装进本批次输出；没有输入记录时跳过。 */
    public void publish(BatchOutput output, OrderOperationResult result) {
        if (result.offsets().isEmpty()) {
            return;
        }
        output.sendBatchMessage(topic, result.offsets(), OrderOffsetSeqBatch::new);
    }
}
