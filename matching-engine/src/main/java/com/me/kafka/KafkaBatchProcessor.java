package com.me.kafka;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 一条消费链路的批处理逻辑。
 *
 * <p>{@link BatchOutput} 由 runner 为当前批次显式创建并传入；有状态 stage 在模块锁内
 * 把输出直接写进去，审计 stage 忽略它（或使用直连模式）。输出与输入的 offset 由 runner
 * 在同一个 Kafka 事务里提交。
 */
@FunctionalInterface
public interface KafkaBatchProcessor<T> {

    void process(List<ConsumerRecord<String, T>> records, BatchOutput output);
}
