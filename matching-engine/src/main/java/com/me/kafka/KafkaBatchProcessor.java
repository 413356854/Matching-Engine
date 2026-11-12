package com.me.kafka;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 一条消费链路的批处理逻辑，签名与现有 listener 方法一致。
 */
@FunctionalInterface
public interface KafkaBatchProcessor<T> {

    void process(List<ConsumerRecord<String, T>> records);
}
