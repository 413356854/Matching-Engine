package com.me.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * 用裸 {@link AdminClient} 创建 6 个 topic（8 分区、RF=1）。
 *
 * <p>后台线程执行、失败每 5s 重试，不阻塞应用启动；已存在的 topic 只校验分区数，
 * 避免 broker 自动创建成默认 1 分区。对应原 Spring Kafka 的 {@code KafkaAdmin} +
 * {@code NewTopic} 自动创建。
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTopicInitializer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicInitializer.class);
    private static final long RETRY_DELAY_MS = 5_000L;

    private final KafkaClientProperties properties;
    private final java.util.concurrent.atomic.AtomicBoolean running =
            new java.util.concurrent.atomic.AtomicBoolean();
    private Thread thread;

    public KafkaTopicInitializer(KafkaClientProperties properties) {
        this.properties = properties;
    }

    @Override
    public void start() {
        if (!properties.admin().autoCreate()) {
            running.set(true);
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        thread = new Thread(this::ensureTopics, "kafka-topic-init");
        thread.setDaemon(true);
        thread.start();
    }

    private void ensureTopics() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                createAndVerify();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (properties.admin().failFast()) {
                    log.error("kafka topic initialization failed and fail-fast is enabled", e);
                    return;
                }
                log.warn("kafka topic initialization failed, retrying in {}ms: {}",
                        RETRY_DELAY_MS, e.toString());
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void createAndVerify() throws Exception {
        List<NewTopic> topics = properties.topics().values().stream()
                .map(name -> new NewTopic(name,
                        properties.topic().partitions(),
                        (short) properties.topic().replicationFactor()))
                .toList();
        try (AdminClient admin = AdminClient.create(
                Map.of(BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers()))) {
            CreateTopicsResult created = admin.createTopics(topics);
            for (NewTopic topic : topics) {
                try {
                    created.values().get(topic.name()).get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw e;
                    }
                }
            }
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(properties.topics().values())
                            .allTopicNames().get(10, TimeUnit.SECONDS);
            for (TopicDescription description : descriptions.values()) {
                int actual = description.partitions().size();
                int expected = properties.topic().partitions();
                if (actual != expected) {
                    log.warn("topic {} has {} partitions, expected {}",
                            description.name(), actual, expected);
                }
            }
        }
    }

    @Override
    public void stop() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
