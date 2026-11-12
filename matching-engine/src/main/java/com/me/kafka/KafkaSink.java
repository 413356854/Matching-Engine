package com.me.kafka;

import java.util.List;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 当前线程绑定的 Kafka 事务型 producer，供 publisher 在事务内发送消息。
 *
 * <p>消费 runner 在调用 listener 前 {@link #bind(KafkaProducer)}，处理结束后
 * {@link #unbind()}；publisher 只调用 {@link #send(String, String, Object)} 或
 * {@link #sendBatch(List)}。REST 下单/充值不走这里，使用 {@link KafkaRestPublisher} 的
 * 幂等非事务 producer。
 *
 * <p><b>批量发送</b>：KafkaProducer 没有“一次发送多条记录”的 API，真正的网络批量由客户端
 * {@code RecordAccumulator} 完成。这里把整个 poll 批次要发送的记录一次性入队（{@link #sendBatch}），
 * 让 producer 按 partition 聚合成 Produce 请求，再由事务的 {@code commitTransaction()} 统一等待。
 * 发送是异步的：错误由 commit/abort 统一暴露；这里只挂日志回调。
 *
 * <p>没有绑定事务时直接抛异常，避免消息被静默发到事务外。
 */
@Component
public class KafkaSink {

    /**
     * 批量消息的默认固定 key。
     *
     * <p>行情、结算等批量 topic 仍使用固定 key，让同一 topic 的所有批量消息落在同一分区，
     * 保持批次之间的生产顺序。冻结回执按 symbol 分组（key=symbol），资金流水按 userId
     * 分组（key=userId），不再使用本常量。
     */
    public static final String BATCH_KEY = "batch";

    /**
     * 单条批量消息最多封装的元素数。
     *
     * <p>与 {@code max-poll-records=2000} 对齐：一个普通消费批次（≤2000 条命令）正好产出
     * 1 条批量消息；若输入本身是批量消息、展开后元素更多，则拆成多条批量消息，避免单条
     * Kafka 消息超过 broker/producer 的 1MB 默认上限。
     */
    public static final int BATCH_CHUNK_SIZE = 2_000;

    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    private final ThreadLocal<KafkaProducer<String, Object>> current = new ThreadLocal<>();

    public void bind(KafkaProducer<String, Object> producer) {
        if (current.get() != null) {
            throw new IllegalStateException("a Kafka producer is already bound to this thread");
        }
        current.set(producer);
    }

    public void unbind() {
        current.remove();
    }

    public void send(String topic, String key, Object value) {
        KafkaProducer<String, Object> producer = requireProducer();
        send(producer, new ProducerRecord<>(topic, key, value));
    }

    /** 显式指定分区发送；账户 slot 必须与 topic 分区一一对应时使用。 */
    public void send(String topic, int partition, String key, Object value) {
        KafkaProducer<String, Object> producer = requireProducer();
        send(producer, new ProducerRecord<>(topic, partition, key, value));
    }

    /**
     * 批量入队；所有记录仍在同一个 Kafka 事务里，由 commitTransaction 一起提交。
     */
    public void sendBatch(List<KafkaMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }
        KafkaProducer<String, Object> producer = requireProducer();
        for (KafkaMessage message : messages) {
            send(producer, new ProducerRecord<>(
                    message.topic(), message.key(), message.value()));
        }
    }

    /**
     * 同一 topic 的批量入队便捷方法：按 keyExtractor 生成每条消息的 key。
     */
    public <T> void sendBatch(String topic, List<T> values, Function<? super T, String> keyExtractor) {
        if (values.isEmpty()) {
            return;
        }
        KafkaProducer<String, Object> producer = requireProducer();
        for (T value : values) {
            send(producer, new ProducerRecord<>(topic, keyExtractor.apply(value), value));
        }
    }

    /**
     * 把一组元素封装成批量消息入队，使用固定 key {@link #BATCH_KEY}；超过
     * {@link #BATCH_CHUNK_SIZE} 时拆成多条，每条都在当前 Kafka 事务内提交。
     */
    public <T> void sendBatchMessage(String topic, List<T> values,
                                     Function<? super List<T>, Object> wrapper) {
        sendBatchMessage(topic, BATCH_KEY, values, wrapper);
    }

    /**
     * 把一组元素封装成批量消息入队，使用指定 key；超过 {@link #BATCH_CHUNK_SIZE} 时拆成多条。
     *
     * <p>key 决定消息落到哪个分区。按 symbol 分组的 topic（如冻结回执）用 symbol 做 key，
     * 同一 symbol 的批次保持顺序且可以按分区并行消费。
     *
     * @param wrapper 把子列表封装成批量消息对象（如 {@code OrderFreezeResultBatch::new}）
     */
    public <T> void sendBatchMessage(String topic, String key, List<T> values,
                                     Function<? super List<T>, Object> wrapper) {
        if (values == null || values.isEmpty()) {
            return;
        }
        KafkaProducer<String, Object> producer = requireProducer();
        for (int from = 0; from < values.size(); from += BATCH_CHUNK_SIZE) {
            int to = Math.min(from + BATCH_CHUNK_SIZE, values.size());
            send(producer, new ProducerRecord<>(
                    topic, key, wrapper.apply(values.subList(from, to))));
        }
    }

    /**
     * 显式指定分区的批量消息；账户输入 topic 用 {@code partition=slot} 把同一用户的
     * 命令固定到一个 consumer。
     */
    public <T> void sendBatchMessage(String topic, int partition, String key, List<T> values,
                                     Function<? super List<T>, Object> wrapper) {
        if (values == null || values.isEmpty()) {
            return;
        }
        KafkaProducer<String, Object> producer = requireProducer();
        for (int from = 0; from < values.size(); from += BATCH_CHUNK_SIZE) {
            int to = Math.min(from + BATCH_CHUNK_SIZE, values.size());
            send(producer, new ProducerRecord<>(
                    topic, partition, key, wrapper.apply(values.subList(from, to))));
        }
    }

    private KafkaProducer<String, Object> requireProducer() {
        KafkaProducer<String, Object> producer = current.get();
        if (producer == null) {
            throw new IllegalStateException(
                    "no Kafka transaction bound to thread; publisher must run inside a "
                            + "KafkaConsumerRunner batch");
        }
        return producer;
    }

    private void send(KafkaProducer<String, Object> producer, ProducerRecord<String, Object> record) {
        producer.send(record, (metadata, error) -> {
            if (error != null) {
                log.error("Kafka send failed asynchronously: topic={}, key={}",
                        record.topic(), record.key(), error);
            }
        });
    }

    /** 一条待发送消息；用于跨 topic 的批量入队。 */
    public record KafkaMessage(String topic, String key, Object value) {
    }
}
