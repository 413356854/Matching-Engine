package com.me.websocket.service;

import com.me.websocket.dto.OrderBookSubscriptionCommand;
import com.me.websocket.vo.OrderBookMessage;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * WebSocket 消息编解码。
 *
 * <p>使用独立的 {@link JsonMapper}，不依赖 Kafka / REST 使用的序列化器；
 * 关闭未知字段失败，方便客户端在协议中追加字段而不影响旧服务端。
 */
@Component
public class OrderBookMessageCodec {

    private static final JsonMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public OrderBookSubscriptionCommand readCommand(String payload) {
        return MAPPER.readValue(payload, OrderBookSubscriptionCommand.class);
    }

    public String write(OrderBookMessage message) {
        return MAPPER.writeValueAsString(message);
    }
}
