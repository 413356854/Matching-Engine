package com.me.kafka;

import org.apache.fory.Fory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * 用 Apache Fory 序列化 Kafka value。
 *
 * <p>每个 producer 持有一个独立的 {@link Fory}；Fory 实例不是线程安全的，
 * 调用方必须保证同一 producer 不被多个线程同时 send。
 * 不写 {@code __TypeId__} 头，消费端按 topic 固定目标类型反序列化。
 */
public final class KafkaForySerializer implements Serializer<Object> {

    private final Fory fory;

    public KafkaForySerializer() {
        this(KafkaFory.newFory());
    }

    /** 测试或每个 producer 可注入独立实例。 */
    public KafkaForySerializer(Fory fory) {
        this.fory = fory;
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        try {
            return fory.serialize(data);
        } catch (RuntimeException e) {
            throw new SerializationException(
                    "failed to Fory-serialize Kafka value for topic " + topic, e);
        }
    }
}
