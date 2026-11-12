package com.me.kafka;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * REST 入口的幂等、<b>非事务</b>发送器。
 *
 * <p>这些请求只有一条命令（下单/充值），没有 consumer offset 需要原子提交，因此不需要
 * Kafka 事务；producer 池使用 {@code enable.idempotence=true} + {@code acks=all}。
 * 充值等需要「已持久化」语义的调用使用 {@link #send} 同步等待 ack；下单使用
 * {@link #sendAsync}，只把记录放入 producer 缓冲区，不等待 broker ack。
 *
 * <p>下游消费端仍使用 {@code read_committed}：非事务消息对 read_committed 消费者永远可见，
 * 不会冲突；若同一分区混有未提交的事务消息，非事务消息只会等到该事务结束（LSO），不会丢或乱序。
 */
@Component
public class KafkaRestPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaRestPublisher.class);

    private final KafkaClientFactory clientFactory;
    private final KafkaClientProperties properties;
    private final long restLingerMs;
    private final int restBatchSizeBytes;
    private final BlockingQueue<KafkaProducer<String, Object>> available = new LinkedBlockingQueue<>();
    private final List<KafkaProducer<String, Object>> allProducers =
            Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger liveProducers = new AtomicInteger();
    private final AtomicInteger nextIndex = new AtomicInteger();
    private volatile boolean closed;

    public KafkaRestPublisher(KafkaClientFactory clientFactory,
                              KafkaClientProperties properties,
                              @Value("${app.kafka.rest-producer-linger-ms:2}") long restLingerMs,
                              @Value("${app.kafka.rest-producer-batch-size-bytes:32768}")
                              int restBatchSizeBytes) {
        this.clientFactory = clientFactory;
        this.properties = properties;
        this.restLingerMs = restLingerMs;
        this.restBatchSizeBytes = restBatchSizeBytes;
    }

    /**
     * 同步发送一条命令并等待 ack；失败抛出 IllegalStateException，由 controller 返回 503。
     */
    public void send(String topic, String key, Object value) {
        send(topic, null, key, value);
    }

    /**
     * 同步发送并显式指定分区；{@code partition == null} 时由 producer 默认分区器决定。
     * 账户资金命令用 {@code partition = slot(userId)} 与账户锁对齐。
     */
    public void send(String topic, Integer partition, String key, Object value) {
        if (closed) {
            throw new IllegalStateException("KafkaRestPublisher is closed");
        }
        KafkaProducer<String, Object> producer = borrow();
        boolean reusable = true;
        try {
            ProducerRecord<String, Object> record = partition == null
                    ? new ProducerRecord<>(topic, key, value)
                    : new ProducerRecord<>(topic, partition, key, value);
            producer.send(record)
                    .get(properties.restSendTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while publishing Kafka record: topic=" + topic + ", key=" + key, e);
        } catch (ExecutionException e) {
            reusable = !isFatal(e.getCause());
            throw new IllegalStateException(
                    "Kafka publish failed: topic=" + topic + ", key=" + key, e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                    "Kafka publish timed out: topic=" + topic + ", key=" + key, e);
        } catch (RuntimeException e) {
            reusable = !isFatal(e);
            throw e;
        } finally {
            release(producer, reusable);
        }
    }

    /**
     * 异步发送一条命令：只确保记录进入 producer 缓冲区，不等待 broker ack。
     *
     * <p>发送失败（broker 不可用、超时等）在回调中记录日志，调用方无法感知；只有同步入队
     * 失败（producer 已关闭、序列化失败、缓冲区满等）会抛出异常。
     */
    public void sendAsync(String topic, String key, Object value) {
        if (closed) {
            throw new IllegalStateException("KafkaRestPublisher is closed");
        }
        KafkaProducer<String, Object> producer = borrow();
        boolean reusable = true;
        try {
            producer.send(new ProducerRecord<>(topic, key, value), (metadata, error) -> {
                if (error != null) {
                    log.error("Kafka async publish failed: topic={}, key={}",
                            topic, key, error);
                }
            });
        } catch (RuntimeException e) {
            reusable = !isFatal(e);
            throw new IllegalStateException(
                    "Kafka async publish failed: topic=" + topic + ", key=" + key, e);
        } finally {
            release(producer, reusable);
        }
    }

    private KafkaProducer<String, Object> borrow() {
        int poolSize = Math.max(1, properties.restProducerPoolSize());
        KafkaProducer<String, Object> producer = available.poll();
        if (producer != null) {
            return producer;
        }
        if (liveProducers.incrementAndGet() <= poolSize) {
            int index = nextIndex.getAndIncrement();
            try {
                producer = clientFactory.createIdempotentProducer(
                        "rest", index, restLingerMs, restBatchSizeBytes);
            } catch (RuntimeException e) {
                liveProducers.decrementAndGet();
                throw e;
            }
            allProducers.add(producer);
            return producer;
        }
        liveProducers.decrementAndGet();
        try {
            producer = available.poll(properties.restBorrowTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for a Kafka producer", e);
        }
        if (producer == null) {
            throw new IllegalStateException("no Kafka REST producer available within "
                    + properties.restBorrowTimeoutMs() + "ms");
        }
        return producer;
    }

    private void release(KafkaProducer<String, Object> producer, boolean reusable) {
        if (reusable) {
            available.offer(producer);
            return;
        }
        allProducers.remove(producer);
        liveProducers.decrementAndGet();
        closeQuietly(producer);
    }

    private static boolean isFatal(Throwable throwable) {
        return throwable instanceof AuthorizationException
                || throwable instanceof OutOfOrderSequenceException
                || throwable instanceof InvalidProducerEpochException
                || throwable instanceof UnsupportedVersionException;
    }

    private static void closeQuietly(KafkaProducer<String, Object> producer) {
        try {
            producer.close(Duration.ofSeconds(5));
        } catch (RuntimeException e) {
            log.warn("failed to close Kafka producer", e);
        }
    }

    @PreDestroy
    public void close() {
        closed = true;
        List<KafkaProducer<String, Object>> producers;
        synchronized (allProducers) {
            producers = new ArrayList<>(allProducers);
            allProducers.clear();
        }
        available.clear();
        producers.forEach(KafkaRestPublisher::closeQuietly);
    }
}
