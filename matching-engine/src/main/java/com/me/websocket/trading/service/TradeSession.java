package com.me.websocket.trading.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.web.socket.WebSocketSession;

/**
 * 一条 /ws/trade 连接的状态：登录用户、限流窗口和等待 ingress ack 的 pending 请求。
 *
 * <p>登录用户只在 {@code login} 时绑定；订单消息不允许覆盖。pending 以 requestId 为键，
 * ack / 超时扫描 / 连接关闭都会清理对应条目。
 *
 * <p>连接首次收到二进制帧时标记为 SBE 协议（{@link #markBinary()}），之后 login / 下单 /
 * ack / 超时回执都使用二进制帧；文本帧连接继续使用 JSON，兼容现有演示前端。
 *
 * <p>除 {@link #session()} / {@link #connectionId()} 外，状态方法都串行化；交易端点的
 * 并发度很低，简单的 {@code synchronized} 足够，避免为每个连接引入额外锁对象。
 */
public final class TradeSession {

    private final long connectionId;
    private final WebSocketSession session;
    private final int maxOrdersPerSecond;
    private final int maxPendingRequests;
    private final ConcurrentHashMap<String, PendingRequest> pending = new ConcurrentHashMap<>();

    private volatile long userId;
    private volatile boolean binary;
    private long rateWindowStartNanos;
    private int rateCount;

    public TradeSession(long connectionId, WebSocketSession session,
                        int maxOrdersPerSecond, int maxPendingRequests) {
        this.connectionId = connectionId;
        this.session = session;
        this.maxOrdersPerSecond = maxOrdersPerSecond;
        this.maxPendingRequests = maxPendingRequests;
        this.rateWindowStartNanos = System.nanoTime();
    }

    public long connectionId() {
        return connectionId;
    }

    public WebSocketSession session() {
        return session;
    }

    public long userId() {
        return userId;
    }

    public boolean isLoggedIn() {
        return userId > 0;
    }

    /** 连接首次收到二进制帧后固定为 SBE 二进制协议，后续所有回执都使用二进制帧。 */
    public boolean isBinary() {
        return binary;
    }

    public void markBinary() {
        binary = true;
    }

    /**
     * 绑定登录用户。
     *
     * @return true 表示首次登录或重复登录同一用户；false 表示连接已绑定其他用户
     */
    public synchronized boolean login(long userId) {
        if (this.userId > 0) {
            return this.userId == userId;
        }
        this.userId = userId;
        return true;
    }

    /** 单连接每秒下单/撤单请求限流；{@code maxOrdersPerSecond<=0} 表示不限制。 */
    public synchronized boolean tryAcquireRateLimit() {
        if (maxOrdersPerSecond <= 0) {
            return true;
        }
        long now = System.nanoTime();
        if (now - rateWindowStartNanos >= 1_000_000_000L) {
            rateWindowStartNanos = now;
            rateCount = 0;
        }
        if (rateCount >= maxOrdersPerSecond) {
            return false;
        }
        rateCount++;
        return true;
    }

    /** 登记一个等待 ack 的请求。 */
    public synchronized PendingAddResult addPending(String requestId, long deadlineNanos) {
        if (pending.containsKey(requestId)) {
            return PendingAddResult.DUPLICATE;
        }
        if (pending.size() >= maxPendingRequests) {
            return PendingAddResult.LIMIT;
        }
        pending.put(requestId, new PendingRequest(deadlineNanos));
        return PendingAddResult.ADDED;
    }

    /** 收到 ack 时移除 pending；没有对应 pending（已超时 / 已断开）返回 null。 */
    public synchronized PendingRequest removePending(String requestId) {
        return pending.remove(requestId);
    }

    /** 移除超过 deadline 的 pending，返回 requestId 列表供调用方发超时通知。 */
    public synchronized List<String> removeExpired(long nowNanos) {
        if (pending.isEmpty()) {
            return List.of();
        }
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, PendingRequest> entry : pending.entrySet()) {
            if (nowNanos >= entry.getValue().deadlineNanos()) {
                expired.add(entry.getKey());
                pending.remove(entry.getKey());
            }
        }
        return expired;
    }

    public int pendingCount() {
        return pending.size();
    }

    /** pending 登记结果。 */
    public enum PendingAddResult {
        ADDED,
        DUPLICATE,
        LIMIT
    }

    /** 一个等待 ack 的请求。 */
    public record PendingRequest(long deadlineNanos) {
    }
}
