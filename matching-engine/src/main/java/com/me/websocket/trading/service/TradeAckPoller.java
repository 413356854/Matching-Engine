package com.me.websocket.trading.service;

import com.me.aeron.AeronCodec;
import com.me.aeron.AeronMediaDriver;
import com.me.aeron.AeronProperties;
import com.me.shared.gateway.AeronOrderAck;
import com.me.websocket.trading.vo.TradeServerMessage;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * WS 侧的 ingress ack 订阅者：每个 ack stream 一个 poll 线程，按 connectionId 找到连接、
 * 按 requestId 清理 pending，再把 ack 推给客户端。
 *
 * <p><b>批量回执</b>：一次 {@link Subscription#poll} 最多读取 {@value #FRAGMENT_LIMIT} 个
 * fragment，按 connectionId 聚合后每连接只写一个 WebSocket 帧。二进制连接编码成 SBE
 * {@code AckBatch}（单帧条数受 {@code ack-batch-size} 和 {@code max-message-bytes} 限制），
 * 文本连接仍逐条发送 JSON。批处理只在同一次 poll 内做，不额外等待，因此吞吐提高而
 * 不引入人为延迟。
 *
 * <p>WS 侧按 connectionId 选择 request/ack stream 分片（见 {@link TradeAeronPublisher}），
 * 同一条连接的所有 ack 固定由一个 poller 处理，连接内顺序不变；多条连接分摊到 N 个线程，
 * 避免单线程 ack 转发成为瓶颈。
 *
 * <p>phase=62：早于 Web 服务器启动，晚于请求发布器（61）停止。
 */
public final class TradeAckPoller implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TradeAckPoller.class);

    /** 单次 poll 的 fragment 上限；越大，同一次 poll 内可聚合的 ack 越多。 */
    private static final int FRAGMENT_LIMIT = 256;
    private static final long STOP_TIMEOUT_SECONDS = 5;

    private final AeronMediaDriver mediaDriver;
    private final AeronProperties properties;
    private final AeronCodec codec;
    private final TradeMessageCodec messageCodec;
    private final TradeBinaryCodec binaryCodec;
    private final TradeSessionRegistry registry;
    private final TradeSessionSender sender;
    private final int streamCount;
    private final int ackBatchSize;
    private final int maxMessageBytes;
    private final ThreadLocal<Map<Long, List<AeronOrderAck>>> pendingBatches =
            ThreadLocal.withInitial(HashMap::new);

    private volatile Subscription[] subscriptions;
    private volatile Thread[] pollerThreads;
    private volatile boolean running;

    public TradeAckPoller(AeronMediaDriver mediaDriver, AeronProperties properties,
                          AeronCodec codec, TradeMessageCodec messageCodec,
                          TradeBinaryCodec binaryCodec,
                          TradeSessionRegistry registry, TradeSessionSender sender,
                          int streamCount,
                          int ackBatchSize, int maxMessageBytes) {
        this.mediaDriver = mediaDriver;
        this.properties = properties;
        this.codec = codec;
        this.messageCodec = messageCodec;
        this.binaryCodec = binaryCodec;
        this.registry = registry;
        this.sender = sender;
        this.streamCount = Math.max(1, streamCount);
        this.ackBatchSize = Math.max(1, ackBatchSize);
        this.maxMessageBytes = maxMessageBytes;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        Subscription[] subs = new Subscription[streamCount];
        try {
            for (int i = 0; i < streamCount; i++) {
                subs[i] = mediaDriver.aeron().addSubscription(
                        properties.channel(), properties.ackStreamId() + i);
            }
        } catch (RuntimeException e) {
            for (Subscription subscription : subs) {
                CloseHelper.quietClose(subscription);
            }
            throw e;
        }
        subscriptions = subs;
        Thread[] threads = new Thread[streamCount];
        pollerThreads = threads;
        running = true;
        for (int i = 0; i < streamCount; i++) {
            final int index = i;
            Thread thread = new Thread(() -> pollLoop(index),
                    "aeron-trade-ack-poller-" + index);
            thread.setDaemon(true);
            threads[index] = thread;
            thread.start();
        }
        log.debug("trade ack pollers started: channel={}, baseStreamId={}, count={}",
                properties.channel(), properties.ackStreamId(), streamCount);
    }

    private void pollLoop(int index) {
        Subscription subscription = subscriptions[index];
        FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        while (running) {
            try {
                int fragments = subscription.poll(assembler, FRAGMENT_LIMIT);
                flushBatches();
                if (fragments > 0) {
                    idleStrategy.reset();
                } else {
                    idleStrategy.idle();
                }
            } catch (RuntimeException e) {
                if (running) {
                    log.error("trade ack poll failed: streamIndex={}", index, e);
                }
                idleStrategy.idle();
            }
        }
        flushBatches();
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        AeronOrderAck ack;
        try {
            ack = codec.decodeAck(buffer, offset, length);
        } catch (RuntimeException e) {
            log.error("failed to decode trade ack: length={}", length, e);
            return;
        }
        TradeSession state = registry.get(ack.connectionId());
        if (state == null) {
            log.debug("ack for closed connection dropped: connectionId={}, requestId={}",
                    ack.connectionId(), ack.requestId());
            return;
        }
        if (state.removePending(ack.requestId()) == null) {
            log.debug("late or unknown ack dropped: connectionId={}, requestId={}",
                    ack.connectionId(), ack.requestId());
            return;
        }

        if (state.isBinary()) {
            pendingBatches.get()
                    .computeIfAbsent(ack.connectionId(), ignored -> new ArrayList<>())
                    .add(ack);
        } else {
            sender.send(ack.connectionId(), messageCodec.write(TradeServerMessage.orderAck(
                    ack.requestId(), ack.success(), ack.reason(), ack.orderId(), ack.symbol())));
        }
    }

    /**
     * 把本线程本次 poll 聚合的 ack 按连接发送：二进制连接一帧最多
     * {@code ack-batch-size} 条、且不超过 {@code max-message-bytes}。
     */
    private void flushBatches() {
        Map<Long, List<AeronOrderAck>> batches = pendingBatches.get();
        if (batches.isEmpty()) {
            return;
        }
        try {
            for (Map.Entry<Long, List<AeronOrderAck>> entry : batches.entrySet()) {
                long connectionId = entry.getKey();
                List<AeronOrderAck> acks = entry.getValue();
                int start = 0;
                while (start < acks.size()) {
                    int end = binaryCodec.ackBatchEnd(
                            acks, start, ackBatchSize, maxMessageBytes);
                    List<AeronOrderAck> chunk = acks.subList(start, end);
                    sender.sendBinary(connectionId, binaryCodec.writeAckBatch(chunk));
                    start = end;
                }
            }
        } finally {
            // 编码 / 发送异常时不保留已路由的 ack，避免下一次 poll 重复回执。
            batches.clear();
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        Subscription[] subs = subscriptions;
        subscriptions = null;
        if (subs != null) {
            for (Subscription subscription : subs) {
                CloseHelper.quietClose(subscription);
            }
        }

        Thread[] threads = pollerThreads;
        pollerThreads = null;
        if (threads != null) {
            for (Thread thread : threads) {
                if (thread == null) {
                    continue;
                }
                thread.interrupt();
                try {
                    thread.join(TimeUnit.SECONDS.toMillis(STOP_TIMEOUT_SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        log.debug("trade ack pollers stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 62;
    }
}
