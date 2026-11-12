package com.me.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * 用 Jackson 3 序列化 Kafka value；不写 {@code __TypeId__} 头，保持纯 JSON 格式。
 *
 * <p>对应原 Spring Kafka {@code JacksonJsonSerializer}；null 值序列化为 null（Kafka tombstone
 * 语义）。
 */
public final class KafkaJsonSerializer implements Serializer<Object> {

    private final JsonMapper mapper;

    public KafkaJsonSerializer(JsonMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JacksonException e) {
            throw new SerializationException("failed to serialize Kafka value for topic " + topic, e);
        }
    }
}
