package com.me.websocket.trading.service;

import com.me.websocket.config.WebSocketProperties;
import com.me.websocket.trading.config.TradingProperties;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

/**
 * /ws/trade 连接注册表：分配 connectionId、保存登录态 / pending，并统一包装线程安全发送会话。
 *
 * <p>connectionId 由 {@code instanceId << 48 | 本地序号} 生成；当前单实例部署时高位固定，
 * 后续多实例部署可直接用于 ack 路由。
 */
public final class TradeSessionRegistry {

    /** WebSocket 会话属性中保存 connectionId 的键。 */
    public static final String CONNECTION_ID_ATTRIBUTE = "tradeConnectionId";

    private final ConcurrentHashMap<Long, TradeSession> sessions = new ConcurrentHashMap<>();
    private final AtomicLong connectionSequence = new AtomicLong();
    private final int instanceId;
    private final int maxOrdersPerSecond;
    private final int maxPendingRequests;
    private final int sendTimeLimitMs;
    private final int sendBufferSizeBytes;

    public TradeSessionRegistry(TradingProperties properties, WebSocketProperties webSocketProperties) {
        this.instanceId = properties.instanceId();
        this.maxOrdersPerSecond = properties.maxOrdersPerSecond();
        this.maxPendingRequests = properties.maxPendingRequests();
        this.sendTimeLimitMs = webSocketProperties.sendTimeLimitMs();
        this.sendBufferSizeBytes = webSocketProperties.sendBufferSizeBytes();
    }

    /** 连接建立时注册，并把 connectionId 写入会话属性。 */
    public TradeSession register(WebSocketSession session) {
        WebSocketSession decorated = decorate(session);
        long connectionId = nextConnectionId();
        TradeSession state = new TradeSession(
                connectionId, decorated, maxOrdersPerSecond, maxPendingRequests);
        sessions.put(connectionId, state);
        session.getAttributes().put(CONNECTION_ID_ATTRIBUTE, connectionId);
        return state;
    }

    /** 从会话属性读取 connectionId；未注册返回 0。 */
    public static long connectionId(WebSocketSession session) {
        Object value = session.getAttributes().get(CONNECTION_ID_ATTRIBUTE);
        return value instanceof Long id ? id : 0L;
    }

    public TradeSession get(long connectionId) {
        return connectionId <= 0 ? null : sessions.get(connectionId);
    }

    public void remove(long connectionId) {
        if (connectionId > 0) {
            sessions.remove(connectionId);
        }
    }

    /** 当前连接快照，供 ack 超时扫描使用。 */
    public Collection<TradeSession> sessions() {
        return sessions.values();
    }

    private long nextConnectionId() {
        long local = connectionSequence.incrementAndGet() & 0x0000FFFFFFFFFFFFL;
        return ((long) instanceId << 48) | local;
    }

    private WebSocketSession decorate(WebSocketSession session) {
        if (session instanceof ConcurrentWebSocketSessionDecorator) {
            return session;
        }
        return new ConcurrentWebSocketSessionDecorator(
                session, sendTimeLimitMs, sendBufferSizeBytes);
    }
}
