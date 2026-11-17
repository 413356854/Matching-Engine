package com.me.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 委托 / 账户两个模块的 FIFO 事务提交管线装配。
 *
 * <p>每个模块一个 {@link ModuleCommitPipeline}：独立的 ArrayBlockingQueue 和唯一的
 * committer 线程；批次锁仍由 OperationService 持有，pipeline 不再接触模块锁。队列容量与单事务合并包数可配置：
 * {@code app.kafka.commit.queue-capacity}（默认 32）、
 * {@code app.kafka.commit.max-packages-per-transaction}（默认 2）。
 *
 * <p>管线实现 {@link org.springframework.context.SmartLifecycle}，phase=50，早于
 * {@link KafkaConsumerManager}（phase=100）启动、晚于它停止。
 */
@Configuration
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class CommitPipelineConfiguration {

    @Bean
    public ModuleCommitPipeline orderCommitPipeline(
            KafkaClientFactory clientFactory,
            FatalState fatalState,
            @Value("${app.kafka.commit.queue-capacity:32}") int queueCapacity,
            @Value("${app.kafka.commit.max-packages-per-transaction:2}")
            int maxPackagesPerTransaction) {
        return new ModuleCommitPipeline("order-committer", clientFactory, fatalState,
                queueCapacity, maxPackagesPerTransaction);
    }

    @Bean
    public ModuleCommitPipeline accountCommitPipeline(
            KafkaClientFactory clientFactory,
            FatalState fatalState,
            @Value("${app.kafka.commit.queue-capacity:32}") int queueCapacity,
            @Value("${app.kafka.commit.max-packages-per-transaction:2}")
            int maxPackagesPerTransaction) {
        return new ModuleCommitPipeline("account-committer", clientFactory, fatalState,
                queueCapacity, maxPackagesPerTransaction);
    }
}
