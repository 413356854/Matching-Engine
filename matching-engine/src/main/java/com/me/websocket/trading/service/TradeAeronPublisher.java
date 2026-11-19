package com.me.websocket.trading.service;

import com.me.aeron.AeronCodec;
import com.me.aeron.AeronMediaDriver;
import com.me.aeron.AeronProperties;
import com.me.shared.gateway.AeronOrderRequest;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * /ws/trade → 币币模块的 Aeron 发布器。
 *
 * <p>使用 N 个 {@link ConcurrentPublication} 按 connectionId 分片：同一连接固定落在一个
 * request stream 上，连接内顺序不变；不同连接分摊到多个币币 poller 线程。每个 publication
 * 支持 Tomcat 线程并发 offer；Fory 和编码缓冲都是 {@link ThreadLocal}。
 *
 * <p>phase=61：晚于 media driver（55）和币币消费者（60），早于 Web 服务器。
 */
public final class TradeAeronPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TradeAeronPublisher.class);

    private final AeronMediaDriver mediaDriver;
    private final AeronProperties properties;
    private final AeronCodec codec;
    private final int streamCount;
    private final ThreadLocal<ExpandableArrayBuffer> buffers =
            ThreadLocal.withInitial(() -> new ExpandableArrayBuffer(1024));

    private volatile ConcurrentPublication[] publications;
    private volatile boolean running;

    public TradeAeronPublisher(AeronMediaDriver mediaDriver, AeronProperties properties,
                               AeronCodec codec, int streamCount) {
        this.mediaDriver = mediaDriver;
        this.properties = properties;
        this.codec = codec;
        this.streamCount = Math.max(1, streamCount);
    }

    /** 该连接固定使用的 stream 下标；WS handler 用它同时选择 request 和 ack stream。 */
    public int streamIndex(long connectionId) {
        return (int) Math.floorMod(connectionId, streamCount);
    }

    /** 尝试发送一条请求；任何失败都不抛异常，由调用方转成客户端错误。 */
    public OfferResult offer(AeronOrderRequest request) {
        ConcurrentPublication[] current = publications;
        if (!running || current == null) {
            return OfferResult.NOT_CONNECTED;
        }
        ConcurrentPublication publication = current[streamIndex(request.connectionId())];
        if (publication == null) {
            return OfferResult.NOT_CONNECTED;
        }
        long result;
        try {
            ExpandableArrayBuffer buffer = buffers.get();
            int length = codec.encodeRequest(request, buffer, 0);
            result = publication.offer(buffer, 0, length);
        } catch (RuntimeException e) {
            log.error("failed to offer aeron order request: requestId={}",
                    request.requestId(), e);
            return OfferResult.ERROR;
        }
        if (result >= 0) {
            return OfferResult.OFFERED;
        }
        if (result == Publication.BACK_PRESSURED) {
            return OfferResult.BACK_PRESSURED;
        }
        if (result == Publication.NOT_CONNECTED) {
            return OfferResult.NOT_CONNECTED;
        }
        if (result == Publication.CLOSED) {
            return OfferResult.CLOSED;
        }
        if (result == Publication.ADMIN_ACTION) {
            return OfferResult.ADMIN_ACTION;
        }
        if (result == Publication.MAX_POSITION_EXCEEDED) {
            return OfferResult.MAX_POSITION_EXCEEDED;
        }
        log.error("unexpected aeron offer result: {}, requestId={}",
                result, request.requestId());
        return OfferResult.ERROR;
    }

    public boolean isConnected() {
        ConcurrentPublication[] current = publications;
        if (current == null) {
            return false;
        }
        for (ConcurrentPublication publication : current) {
            if (publication == null || !publication.isConnected()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        ConcurrentPublication[] created = new ConcurrentPublication[streamCount];
        try {
            for (int i = 0; i < streamCount; i++) {
                created[i] = mediaDriver.aeron().addPublication(
                        properties.channel(), properties.requestStreamId() + i);
            }
        } catch (RuntimeException e) {
            for (ConcurrentPublication publication : created) {
                if (publication != null) {
                    publication.close();
                }
            }
            log.error("failed to create aeron order publications", e);
            throw new IllegalStateException("failed to create aeron order publications", e);
        }
        publications = created;
        running = true;
        log.debug("trade aeron publishers started: channel={}, baseStreamId={}, count={}",
                properties.channel(), properties.requestStreamId(), streamCount);
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        ConcurrentPublication[] current = publications;
        publications = null;
        if (current != null) {
            for (ConcurrentPublication publication : current) {
                if (publication != null) {
                    publication.close();
                }
            }
        }
        log.debug("trade aeron publishers stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 61;
    }

    /** offer 结果。 */
    public enum OfferResult {
        OFFERED,
        BACK_PRESSURED,
        NOT_CONNECTED,
        CLOSED,
        ADMIN_ACTION,
        MAX_POSITION_EXCEEDED,
        ERROR
    }
}
