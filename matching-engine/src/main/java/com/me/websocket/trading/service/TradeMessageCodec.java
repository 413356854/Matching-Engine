package com.me.websocket.trading.service;

import com.me.websocket.trading.dto.TradeClientMessage;
import com.me.websocket.trading.vo.TradeServerMessage;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * /ws/trade 的 JSON 编解码。
 *
 * <p>使用独立 mapper，不依赖盘口 WebSocket / Kafka / REST 的序列化器；忽略未知字段，
 * 便于客户端在协议中追加字段而不影响旧服务端。
 */
@Component
public class TradeMessageCodec {

    private static final JsonMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public TradeClientMessage read(String payload) {
        return MAPPER.readValue(payload, TradeClientMessage.class);
    }

    public String write(TradeServerMessage message) {
        return MAPPER.writeValueAsString(message);
    }
}
