package com.me.websocket.service;

import com.me.websocket.config.WebSocketProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

/**
 * 盘口订阅注册表：保存每个 WebSocket 连接的当前订阅。
 *
 * <p>一个连接同一时刻只订阅一个交易对；再次 {@link #subscribe} 会用新交易对替换旧订阅，
 * 对应前端切换交易对时“重新订阅”的行为。会话统一包装为
 * {@link ConcurrentWebSocketSessionDecorator}，因为定时推送线程与 WebSocket 容器线程
 * 可能同时向同一个连接写数据，而底层 session 不保证发送线程安全。
 */
@Component
public class OrderBookSubscriptionRegistry {

    private final int sendTimeLimitMs;
    private final int sendBufferSizeBytes;
    private final ConcurrentMap<String, SessionState> sessions = new ConcurrentHashMap<>();

    public OrderBookSubscriptionRegistry(WebSocketProperties properties) {
        this.sendTimeLimitMs = properties.sendTimeLimitMs();
        this.sendBufferSizeBytes = properties.sendBufferSizeBytes();
    }

    /** 连接建立时注册；返回用于发送的线程安全会话。 */
    public WebSocketSession register(WebSocketSession session) {
        return stateOf(session).session;
    }

    /** 返回该连接的线程安全发送会话；未注册时原样返回。 */
    public WebSocketSession session(WebSocketSession session) {
        SessionState state = sessions.get(session.getId());
        return state == null ? session : state.session;
    }

    /**
     * 订阅或重新订阅交易对。
     *
     * @return 该连接的线程安全发送会话，调用方可以直接回执和立即推送首帧
     */
    public WebSocketSession subscribe(WebSocketSession session, String symbol, int depth) {
        SessionState state = stateOf(session);
        state.subscription = new Subscription(symbol, depth);
        return state.session;
    }

    /** 清空订阅但保留连接，客户端之后仍可重新订阅。 */
    public void unsubscribe(WebSocketSession session) {
        SessionState state = sessions.get(session.getId());
        if (state != null) {
            state.subscription = null;
        }
    }

    /** 连接关闭 / 传输错误时移除会话。 */
    public void remove(WebSocketSession session) {
        sessions.remove(session.getId());
    }

    /** 当前所有有效订阅的只读快照，供定时推送任务分组使用。 */
    public List<Subscriber> subscribers() {
        List<Subscriber> subscribers = new ArrayList<>(sessions.size());
        for (SessionState state : sessions.values()) {
            Subscription subscription = state.subscription;
            if (subscription != null) {
                subscribers.add(new Subscriber(
                        state.session, subscription.symbol, subscription.depth));
            }
        }
        return subscribers;
    }

    private SessionState stateOf(WebSocketSession session) {
        return sessions.computeIfAbsent(session.getId(), id -> new SessionState(decorate(session)));
    }

    private WebSocketSession decorate(WebSocketSession session) {
        if (session instanceof ConcurrentWebSocketSessionDecorator) {
            return session;
        }
        return new ConcurrentWebSocketSessionDecorator(
                session, sendTimeLimitMs, sendBufferSizeBytes);
    }

    private record Subscription(String symbol, int depth) {
    }

    /** 一条有效订阅。 */
    public record Subscriber(WebSocketSession session, String symbol, int depth) {
    }

    private static final class SessionState {

        private final WebSocketSession session;
        private volatile Subscription subscription;

        private SessionState(WebSocketSession session) {
            this.session = session;
        }
    }
}
