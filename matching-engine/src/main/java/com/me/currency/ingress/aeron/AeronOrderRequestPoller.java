package com.me.currency.ingress.aeron;

import com.me.aeron.AeronCodec;
import com.me.aeron.AeronMediaDriver;
import com.me.aeron.AeronProperties;
import com.me.shared.gateway.AeronOrderAck;
import com.me.shared.gateway.AeronOrderRequest;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * 币币模块的 Aeron 下单/撤单请求消费者：每个 request stream 一个 poll 线程。
 *
 * <p>同一个 stream 上仍是单线程，保证连接内请求顺序；多个 stream 并行处理不同的连接
 * （WS 侧按 connectionId 选择 request/ack stream 分片），把原先单线程的接收/校验/ack
 * 发布阶段扩展到 N 路。ring 本身是 multi-producer，N 个 poller 提交的顺序就是 ring 的
 * 入队顺序。
 *
 * <p>phase=60：晚于 media driver（55）和 ack 发布器（59），早于 Web 服务器；停止时先于
 * ingress ring（50）停止，避免 ring 关闭后仍有 submit。
 */
public final class AeronOrderRequestPoller implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(AeronOrderRequestPoller.class);

    private static final int FRAGMENT_LIMIT = 10;
    private static final long STOP_TIMEOUT_SECONDS = 5;

    private final AeronMediaDriver mediaDriver;
    private final AeronProperties properties;
    private final AeronCodec codec;
    private final AeronRequestProcessor processor;
    private final AeronAckPublisher ackPublisher;
    private final int streamCount;

    private volatile Subscription[] subscriptions;
    private volatile Thread[] pollerThreads;
    private volatile boolean running;

    public AeronOrderRequestPoller(
            AeronMediaDriver mediaDriver,
            AeronProperties properties,
            AeronCodec codec,
            AeronRequestProcessor processor,
            AeronAckPublisher ackPublisher,
            int streamCount) {
        this.mediaDriver = mediaDriver;
        this.properties = properties;
        this.codec = codec;
        this.processor = processor;
        this.ackPublisher = ackPublisher;
        this.streamCount = Math.max(1, streamCount);
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
                        properties.channel(), properties.requestStreamId() + i);
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
                    "aeron-order-request-poller-" + index);
            thread.setDaemon(true);
            threads[index] = thread;
            thread.start();
        }
        log.debug("aeron order request pollers started: channel={}, baseStreamId={}, count={}",
                properties.channel(), properties.requestStreamId(), streamCount);
    }

    private void pollLoop(int index) {
        Subscription subscription = subscriptions[index];
        FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        while (running) {
            try {
                int fragments = subscription.poll(assembler, FRAGMENT_LIMIT);
                if (fragments > 0) {
                    idleStrategy.reset();
                } else {
                    idleStrategy.idle();
                }
            } catch (RuntimeException e) {
                if (running) {
                    log.error("aeron order request poll failed: streamIndex={}", index, e);
                }
                idleStrategy.idle();
            }
        }
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        AeronOrderRequest request;
        try {
            request = codec.decodeRequest(buffer, offset, length);
        } catch (RuntimeException e) {
            log.error("failed to decode aeron order request: length={}", length, e);
            return;
        }

        AeronOrderAck ack = processor.process(request);
        if (ack == null) {
            return;
        }
        int replyStreamId = request.replyStreamId() > 0
                ? request.replyStreamId()
                : properties.ackStreamId();
        ackPublisher.publish(replyStreamId, ack);
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
        log.debug("aeron order request pollers stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 60;
    }
}
