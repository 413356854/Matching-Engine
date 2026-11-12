package com.me.kafka;

import java.util.List;

/**
 * 所有消费 runner 的集合。
 *
 * <p>{@link KafkaConsumerConfiguration} 按 {@code app.kafka.consumer.concurrency} 为每个
 * stage 创建一个或多个 runner，再由 {@link KafkaConsumerManager} 一条 runner 一个线程启动。
 * 用独立的 holder 而不是直接注入 {@code List<KafkaConsumerRunner>}，避免与 Spring 的
 * 集合注入语义（按元素类型收集 Bean）混淆。
 */
public record KafkaConsumerRunners(List<KafkaConsumerRunner<?>> runners) {

    public KafkaConsumerRunners {
        runners = List.copyOf(runners);
    }
}
