package com.me.order.mq;

import com.me.kafka.KafkaSink;
import com.me.order.dto.OrderOffsetSeqBatch;
import com.me.order.dto.OrderOperationResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把「委托输入消费记录 → orderSeq」的映射发往 {@code me.order.offset-seq-batch}。
 *
 * <p>{@link com.me.order.service.OrderOperationService} 在一个批次内分配一个全局序号，
 * 并把批次内每条输入记录（topic / partition / offset）与该序号配对；这里把整批映射
 * 封装成<b>一条</b>批量消息（固定 key {@code batch}，落同一分区），与撮合输出、结算命令、
 * 委托历史及消费 offset 在同一个 Kafka 事务内提交。
 *
 * <p>备份快照恢复后回溯 Kafka 消息时，按 {@code orderSeq} 顺序重放输入消息，
 * 并用快照水位过滤已经执行过的记录。
 */
@Component
public class OrderOffsetSeqPublisher {

    private final KafkaSink sink;
    private final String topic;

    public OrderOffsetSeqPublisher(
            KafkaSink sink,
            @Value("${app.kafka.topics.order-offset-seq}") String topic) {
        this.sink = sink;
        this.topic = topic;
    }

    /** 在当前 Kafka 事务内发送本批的 offset-seq 映射；没有输入记录时跳过。 */
    public void publish(OrderOperationResult result) {
        if (result.offsets().isEmpty()) {
            return;
        }
        sink.sendBatchMessage(topic, result.offsets(), OrderOffsetSeqBatch::new);
    }
}
