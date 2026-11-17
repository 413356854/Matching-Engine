package com.me.websocket.service;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * 统一的 WebSocket 文本发送入口。
 *
 * <p>发送失败或会话已关闭时注销该会话，避免后续推送周期继续向死连接写数据。
 * 慢客户端由 {@code ConcurrentWebSocketSessionDecorator} 的发送超时与缓冲上限保护。
 */
@Component
public class OrderBookSessionSender {

    private static final Logger log = LoggerFactory.getLogger(OrderBookSessionSender.class);

    private final OrderBookSubscriptionRegistry registry;

    public OrderBookSessionSender(OrderBookSubscriptionRegistry registry) {
        this.registry = registry;
    }

    public void send(WebSocketSession session, String payload) {
        WebSocketSession target = registry.session(session);
        if (!target.isOpen()) {
            registry.remove(session);
            return;
        }
        try {
            target.sendMessage(new TextMessage(payload));
        } catch (IOException | RuntimeException ex) {
            registry.remove(session);
            log.debug("order book websocket send failed, session={}: {}",
                    session.getId(), ex.toString());
            closeQuietly(target);
        }
    }

    private static void closeQuietly(WebSocketSession session) {
        try {
            session.close(CloseStatus.SERVER_ERROR);
        } catch (IOException ignored) {
            // 连接已经不可用，关闭失败无需再处理。
        }
    }
}
