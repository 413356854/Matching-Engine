package com.me.currency.ingress.aeron;

import com.me.aeron.AeronCodec;
import com.me.aeron.AeronMediaDriver;
import com.me.aeron.AeronProperties;
import com.me.shared.gateway.AeronOrderAck;
import io.aeron.Publication;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * 币币模块 → WebSocket 的 ingress ack 发布器。
 *
 * <p>按请求携带的 {@code replyStreamId} 缓存 publication；ack 发送是非阻塞的，
 * 未连接 / back-pressure 时直接丢弃并记录 debug 日志，绝不阻塞订单消费线程。
 * 客户端通过 ack 超时感知丢失，然后查询委托状态。
 *
 * <p>phase=59：早于请求消费者（60）启动，晚于 media driver（55）；停止时晚于请求消费者
 * 停止，保证最后一批 ack 仍有发布通道。
 */
public final class AeronAckPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(AeronAckPublisher.class);

    /**
     * ack 发布被 back-pressure 时的最长等待。ack 回程是异步链路的一部分，不能静默丢弃：
     * 等待会阻塞请求 poller，进而让请求 stream 反压 WS offer，形成系统级流控。
     */
    private static final long ACK_PUBLISH_TIMEOUT_NANOS = java.util.concurrent.TimeUnit.SECONDS
            .toNanos(5);

    private final AeronMediaDriver mediaDriver;
    private final AeronProperties properties;
    private final AeronCodec codec;
    private final ConcurrentHashMap<Integer, Publication> publications = new ConcurrentHashMap<>();
    private final ThreadLocal<ExpandableArrayBuffer> buffers =
            ThreadLocal.withInitial(() -> new ExpandableArrayBuffer(512));

    private volatile boolean running;

    public AeronAckPublisher(AeronMediaDriver mediaDriver, AeronProperties properties,
                             AeronCodec codec) {
        this.mediaDriver = mediaDriver;
        this.properties = properties;
        this.codec = codec;
    }

    /**
     * 尽力发送一条 ack。
     *
     * @return true 表示已写入 Aeron term buffer；false 表示未启动、未连接或 back-pressure
     */
    public boolean publish(int replyStreamId, AeronOrderAck ack) {
        if (!running || ack == null) {
            return false;
        }
        int streamId = replyStreamId > 0 ? replyStreamId : properties.ackStreamId();
        Publication publication;
        try {
            publication = publication(streamId);
        } catch (RuntimeException e) {
            log.error("failed to create aeron ack publication: streamId={}", streamId, e);
            return false;
        }

        ExpandableArrayBuffer buffer = buffers.get();
        int length = codec.encodeAck(ack, buffer, 0);
        long result = publication.offer(buffer, 0, length);
        long deadline = System.nanoTime() + ACK_PUBLISH_TIMEOUT_NANOS;
        while (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
            if (!running) {
                return false;
            }
            if (System.nanoTime() >= deadline) {
                log.warn("aeron ack publish timed out: streamId={}, result={}, requestId={}",
                        streamId, result, ack.requestId());
                return false;
            }
            LockSupport.parkNanos(100_000L);
            result = publication.offer(buffer, 0, length);
        }
        if (result < 0) {
            log.debug("aeron ack not delivered: streamId={}, result={}, requestId={}",
                    streamId, result, ack.requestId());
            return false;
        }
        return true;
    }

    private Publication publication(int streamId) {
        Publication current = publications.get(streamId);
        if (current != null) {
            return current;
        }
        Publication created = mediaDriver.aeron().addPublication(properties.channel(), streamId);
        Publication existing = publications.putIfAbsent(streamId, created);
        if (existing != null) {
            created.close();
            return existing;
        }
        return created;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        publications.values().forEach(Publication::close);
        publications.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 59;
    }
}
