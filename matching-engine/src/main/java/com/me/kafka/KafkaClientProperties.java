package com.me.kafka;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 裸 kafka-clients 的配置，替代原 Spring Kafka 的 {@code spring.kafka.*}。
 *
 * <p>topic / group 名称沿用 {@code app.kafka.topics} / {@code app.kafka.groups}，
 * 消费组 ID 与 loadtest 脚本保持一致，不能随意修改。
 */
@ConfigurationProperties("app.kafka")
public record KafkaClientProperties(
        boolean enabled,
        String bootstrapServers,
        String instanceId,
        String transactionIdPrefix,
        long pollTimeoutMs,
        long retryBackoffMs,
        int restProducerPoolSize,
        long restBorrowTimeoutMs,
        long restSendTimeoutMs,
        Consumer consumer,
        Producer producer,
        Admin admin,
        Map<String, String> topics,
        Map<String, String> groups,
        Topic topic) {

    public record Consumer(
            boolean enableAutoCommit,
            String isolationLevel,
            String autoOffsetReset,
            boolean allowAutoCreateTopics,
            int maxPollRecords,
            @DefaultValue("10") int batchMaxPollRecords,
            int fetchMaxWaitMs,
            int fetchMinSizeBytes,
            int maxPollIntervalMs,
            int sessionTimeoutMs,
            int prefetchPollTimeoutMs,
            Map<String, Integer> concurrency) {

        /**
         * 某个 stage 一次 poll 最多返回的 record 数。
         *
         * <p>逐条 topic 使用 {@code maxPollRecords}；批量 topic 的一条 record 内含
         * {@code msgList}，使用更小的 {@code batchMaxPollRecords}，避免一次 poll
         * 把过多元素带进同一个事务。
         */
        public int maxPollRecordsFor(boolean batch) {
            return batch ? batchMaxPollRecords : maxPollRecords;
        }

        /**
         * 某个消费 stage 的并发 consumer 数；未配置或小于 1 时按 1 处理。
         *
         * <p>同一 group 内一个分区只会分给一个 consumer，因此并发数超过分区数时多余
         * consumer 会空闲。按 key 保序的 stage（如 order-command 的 symbol）可以并发，
         * 依赖全局顺序的 stage 必须保持 1。
         */
        public int concurrencyFor(String stage) {
            Integer configured = concurrency == null ? null : concurrency.get(stage);
            return configured == null || configured < 1 ? 1 : configured;
        }
    }

    public record Producer(
            String acks,
            boolean enableIdempotence,
            int transactionTimeoutMs,
            int maxBlockMs,
            int deliveryTimeoutMs,
            int lingerMs,
            int batchSizeBytes) {
    }

    public record Admin(boolean autoCreate, boolean failFast) {
    }

    public record Topic(int partitions, int replicationFactor) {
    }
}
