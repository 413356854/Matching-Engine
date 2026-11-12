package com.me.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * 用 Jackson 3 把 Kafka value 反序列化为固定目标类型。
 *
 * <p>替代原 {@code @KafkaListener(properties = "spring.json.value.default.type=...")}：
 * 每条消费链路在构造时绑定目标类型，不依赖消息头或类型映射。
 *
 * <p>关闭 {@code FAIL_ON_NULL_FOR_PRIMITIVES}：外部生产者 / 旧消息可能省略可选的基础类型字段
 * （例如委托命令的 {@code priceScale} / {@code quantityScale} / {@code quoteScale}），
 * 缺失时按 0 反序列化，再由监听器按交易对配置补齐或校验拒绝。
 */
public final class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private final JsonMapper mapper;
    private final Class<T> type;

    public KafkaJsonDeserializer(JsonMapper mapper, Class<T> type) {
        this.mapper = mapper.rebuild()
                .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .build();
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.readValue(data, type);
        } catch (JacksonException e) {
            throw new SerializationException(
                    "failed to deserialize Kafka value for topic " + topic
                            + " as " + type.getName(), e);
        }
    }
}
