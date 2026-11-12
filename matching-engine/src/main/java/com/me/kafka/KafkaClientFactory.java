package com.me.kafka;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;

/**
 * 按应用配置创建裸 {@link KafkaConsumer} / {@link KafkaProducer}。
 *
 * <p>producer 的 {@code transactional.id} = 前缀 + role + instanceId + 序号，保证同一进程内
 * 每个事务 producer 唯一；多实例部署时必须为每个实例配置不同的 {@code app.kafka.instance-id}。
 * consumer 的 {@code allow.auto.create.topics=false}，避免 broker 以默认 1 分区自动建 topic。
 */
@Component
public class KafkaClientFactory {

    private final KafkaClientProperties properties;

    public KafkaClientFactory(KafkaClientProperties properties) {
        this.properties = properties;
    }

    /**
     * 创建并初始化一个事务型 producer；{@code initTransactions()} 会阻塞到事务协调器可用。
     */
    public KafkaProducer<String, Object> createProducer(String role, int index) {
        Properties props = producerProperties(role, index);
        props.put(TRANSACTIONAL_ID_CONFIG,
                properties.transactionIdPrefix() + role + "-" + properties.instanceId() + "-" + index);
        props.put(TRANSACTION_TIMEOUT_CONFIG, properties.producer().transactionTimeoutMs());
        KafkaProducer<String, Object> producer = new KafkaProducer<>(props,
                new StringSerializer(), new KafkaForySerializer(KafkaFory.newFory()));
        producer.initTransactions();
        return producer;
    }

    /**
     * 创建幂等、非事务 producer（REST 下单/充值使用）。
     *
     * <p>没有 {@code transactional.id}，不做 {@code initTransactions}；依靠
     * {@code enable.idempotence=true} + {@code acks=all} 保证单 producer 会话内的重试幂等。
     */
    public KafkaProducer<String, Object> createIdempotentProducer(String role, int index) {
        return createIdempotentProducer(role, index,
                properties.producer().lingerMs(), properties.producer().batchSizeBytes());
    }

    /**
     * 幂等、非事务 producer，并覆盖 linger / batch.size（REST 下单路径使用）。
     */
    public KafkaProducer<String, Object> createIdempotentProducer(
            String role, int index, long lingerMs, int batchSizeBytes) {
        Properties props = producerProperties(role, index);
        props.put(LINGER_MS_CONFIG, lingerMs);
        props.put(BATCH_SIZE_CONFIG, batchSizeBytes);
        return new KafkaProducer<>(props,
                new StringSerializer(), new KafkaForySerializer(KafkaFory.newFory()));
    }

    private Properties producerProperties(String role, int index) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        props.put(CLIENT_ID_CONFIG, role + "-" + properties.instanceId() + "-" + index);
        props.put(ACKS_CONFIG, properties.producer().acks());
        props.put(ENABLE_IDEMPOTENCE_CONFIG, properties.producer().enableIdempotence());
        props.put(MAX_BLOCK_MS_CONFIG, properties.producer().maxBlockMs());
        props.put(DELIVERY_TIMEOUT_MS_CONFIG, properties.producer().deliveryTimeoutMs());
        props.put(LINGER_MS_CONFIG, properties.producer().lingerMs());
        props.put(BATCH_SIZE_CONFIG, properties.producer().batchSizeBytes());
        return props;
    }

    /**
     * 创建一条消费链路的 consumer；value 反序列化为固定目标类型。
     */
    public <T> KafkaConsumer<String, T> createConsumer(String name, String group, Class<T> type) {
        return createConsumer(name, group, type, properties.consumer().maxPollRecords());
    }

    /**
     * 创建一条消费链路的 consumer，并覆盖全局 {@code max.poll.records}。
     *
     * <p>批量 topic 的一条 record 内含 {@code msgList}，用更小的 poll 上限限制
     * 每次事务处理的批量消息条数。
     */
    public <T> KafkaConsumer<String, T> createConsumer(
            String name, String group, Class<T> type, int maxPollRecords) {
        return createConsumer(name, group, type, maxPollRecords,
                properties.consumer().fetchMaxWaitMs());
    }

    /**
     * 创建一条消费链路的 consumer，同时覆盖 {@code max.poll.records} 与
     * {@code fetch.max.wait.ms}。
     *
     * <p>资金流水这类批量归档 stage 可以拉大 poll 批次、延长 fetch 等待，用更大的
     * 单批减少 QWP ack 与 Kafka commit 的固定开销；其他 stage 保持默认低延迟。
     */
    public <T> KafkaConsumer<String, T> createConsumer(
            String name, String group, Class<T> type, int maxPollRecords, int fetchMaxWaitMs) {
        if (maxPollRecords < 1) {
            throw new IllegalArgumentException("max.poll.records must be >= 1: " + maxPollRecords);
        }
        KafkaClientProperties.Consumer consumer = properties.consumer();
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        props.put(CLIENT_ID_CONFIG, name + "-" + group + "-" + properties.instanceId());
        props.put(GROUP_ID_CONFIG, group);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, consumer.enableAutoCommit());
        props.put(ISOLATION_LEVEL_CONFIG, consumer.isolationLevel());
        props.put(AUTO_OFFSET_RESET_CONFIG, consumer.autoOffsetReset());
        props.put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, consumer.allowAutoCreateTopics());
        props.put(MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        props.put(FETCH_MIN_BYTES_CONFIG, consumer.fetchMinSizeBytes());
        props.put(MAX_POLL_INTERVAL_MS_CONFIG, consumer.maxPollIntervalMs());
        props.put(SESSION_TIMEOUT_MS_CONFIG, consumer.sessionTimeoutMs());
        return new KafkaConsumer<>(props,
                new StringDeserializer(),
                new KafkaForyDeserializer<>(KafkaFory.newFory(), type));
    }
}
