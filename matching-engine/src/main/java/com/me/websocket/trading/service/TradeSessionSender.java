package com.me.websocket.trading.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * /ws/trade 的统一文本发送入口：发送失败或会话已关闭时注销连接。
 *
 * <p>会话在 {@link TradeSessionRegistry#register} 时已经包装为
 * {@code ConcurrentWebSocketSessionDecorator}，Tomcat 线程（即时错误）和 Aeron ack
 * 线程可以安全地同时写入同一条连接。
 */
public final class TradeSessionSender {

    private static final Logger log = LoggerFactory.getLogger(TradeSessionSender.class);

    private final TradeSessionRegistry registry;

    public TradeSessionSender(TradeSessionRegistry registry) {
        this.registry = registry;
    }

    public void send(long connectionId, String payload) {
        send(connectionId, new TextMessage(payload));
    }

    /** 发送 SBE 二进制帧；payload 由调用方独占，发送完成后不再复用。 */
    public void sendBinary(long connectionId, ByteBuffer payload) {
        send(connectionId, new BinaryMessage(payload));
    }

    private void send(long connectionId, WebSocketMessage<?> message) {
        TradeSession state = registry.get(connectionId);
        if (state == null) {
            return;
        }
        WebSocketSession session = state.session();
        if (!session.isOpen()) {
            registry.remove(connectionId);
            return;
        }
        try {
            session.sendMessage(message);
        } catch (IOException | RuntimeException ex) {
            registry.remove(connectionId);
            log.debug("trade websocket send failed, connectionId={}: {}",
                    connectionId, ex.toString());
            closeQuietly(session);
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
