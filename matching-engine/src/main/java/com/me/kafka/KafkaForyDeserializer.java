package com.me.kafka;

import org.apache.fory.Fory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * 用 Apache Fory 把 Kafka value 反序列化为固定目标类型。
 *
 * <p>每个 consumer 持有一个独立的 {@link Fory}；每条消费链路只有一个消费线程，
 * 因此无需 ThreadSafeFory。类必须在 {@link KafkaFory} 中注册。
 */
public final class KafkaForyDeserializer<T> implements Deserializer<T> {

    private final Fory fory;
    private final Class<T> type;

    public KafkaForyDeserializer(Class<T> type) {
        this(KafkaFory.newFory(), type);
    }

    /** 测试或每个 consumer 可注入独立实例。 */
    public KafkaForyDeserializer(Fory fory, Class<T> type) {
        this.fory = fory;
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return fory.deserialize(data, type);
        } catch (RuntimeException e) {
            throw new SerializationException(
                    "failed to Fory-deserialize Kafka value for topic " + topic
                            + " as " + type.getName(), e);
        }
    }
}
