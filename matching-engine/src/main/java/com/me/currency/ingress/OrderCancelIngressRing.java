package com.me.currency.ingress;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.me.shared.order.OrderCancelCommand;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * 撤单命令的独立 Disruptor 入口：HTTP 线程是生产者，单 handler 线程按 symbol 攒批后
 * 批量发 Kafka。
 *
 * <p><b>与下单入口独立</b>：撤单使用自己的 ring、ticker 与 handler 线程，撤单洪峰不会
 * 占用下单聚合线程，下单延迟不受撤单影响。
 *
 * <p><b>批量</b>：handler 维护 {@code Map<symbol, List<OrderCancelCommand>>}；任一 symbol
 * 达到 {@code app.currency.cancel-ingress.batch-size}（默认 1000）或 20ms 定时 tick 到达时，
 * 把 map 中所有 symbol 的撤单分别封装成 {@code OrderCancelBatch}（{@code key=symbol}）发送。
 *
 * <p><b>背压</b>：HTTP 线程用 {@code tryPublishEvent} 非阻塞入队，ring 满或未启动直接抛异常，
 * controller 返回 503；绝不无限排队。handler 线程只做聚合与 producer 入队，不等待 broker ack。
 *
 * <p>生命周期 phase = 50：早于 Web 服务器启动、晚于它停止，保证请求到达时 ring 已就绪，
 * 停机时最后一个请求也能入队并 flush。
 */
public final class OrderCancelIngressRing implements OrderCancelIngress, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelIngressRing.class);

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final OrderCancelIngressProperties properties;
    private final OrderCancelIngressHandler handler;
    private final Disruptor<OrderCancelIngressEvent> disruptor;

    private volatile RingBuffer<OrderCancelIngressEvent> ringBuffer;
    private volatile ScheduledExecutorService ticker;
    private volatile Thread handlerThread;
    private volatile boolean running;

    public OrderCancelIngressRing(OrderCancelIngressProperties properties,
                                  OrderCancelBatchSender sender) {
        this.properties = properties;
        this.handler = new OrderCancelIngressHandler(properties, sender);
        this.disruptor = new Disruptor<>(
                OrderCancelIngressEvent::new,
                properties.ringSize(),
                this::newHandlerThread,
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        this.disruptor.handleEventsWith(handler);
        this.disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());
    }

    private Thread newHandlerThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "order-cancel-ingress-handler");
        thread.setDaemon(true);
        handlerThread = thread;
        return thread;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        ringBuffer = disruptor.start();
        running = true;
        ticker = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "order-cancel-ingress-tick");
            thread.setDaemon(true);
            return thread;
        });
        ticker.scheduleAtFixedRate(this::publishTick,
                properties.flushIntervalMs(), properties.flushIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    /** 定时发布一个空事件，唤醒聚合线程检查 20ms 窗口。 */
    private void publishTick() {
        RingBuffer<OrderCancelIngressEvent> buffer = ringBuffer;
        if (!running || buffer == null) {
            return;
        }
        try {
            buffer.tryPublishEvent((event, sequence) -> event.setTick());
        } catch (RuntimeException ignored) {
            // ring 满时跳过本次 tick，下一轮会再次触发
        }
    }

    @Override
    public void submit(OrderCancelCommand command) {
        RingBuffer<OrderCancelIngressEvent> buffer = ringBuffer;
        if (!running || buffer == null) {
            throw new IllegalStateException("order cancel ingress ring is not running");
        }
        boolean published;
        try {
            published = buffer.tryPublishEvent(
                    (event, sequence, cancel) -> event.set(cancel), command);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "failed to enqueue order cancel command: orderId=" + command.orderId(), e);
        }
        if (!published) {
            throw new IllegalStateException("order cancel ingress ring is full: capacity="
                    + properties.ringSize() + ", orderId=" + command.orderId());
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        ScheduledExecutorService currentTicker = ticker;
        ticker = null;
        if (currentTicker != null) {
            currentTicker.shutdownNow();
        }
        // 先发布停机屏障：handler 处理到它时，之前的撤单都已进入 map 并 flush。
        RingBuffer<OrderCancelIngressEvent> buffer = ringBuffer;
        if (buffer != null) {
            CompletableFuture<Void> drained = publishStopSignal(buffer);
            if (drained != null) {
                try {
                    drained.get(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException | TimeoutException e) {
                    log.warn("order cancel ingress handler did not drain before stop", e);
                }
            }
        }
        try {
            disruptor.shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (com.lmax.disruptor.TimeoutException e) {
            log.warn("order cancel ingress handler did not terminate within {}s; halting",
                    SHUTDOWN_TIMEOUT_SECONDS);
            disruptor.halt();
        }
        Thread thread = handlerThread;
        if (thread != null) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // 只在 handler 线程确实结束后才碰 map，避免超时路径下的并发访问。
        if (thread == null || !thread.isAlive()) {
            handler.flushRemaining();
        } else {
            log.warn("order cancel ingress handler still running; skipping final flush");
        }
    }

    /** 发布停机屏障并返回 handler 处理完它后完成的 future；ring 一直满时返回 null。 */
    private CompletableFuture<Void> publishStopSignal(RingBuffer<OrderCancelIngressEvent> buffer) {
        CompletableFuture<Void> drained = new CompletableFuture<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            boolean published;
            try {
                published = buffer.tryPublishEvent(
                        (event, sequence, future) -> event.setStop(future), drained);
            } catch (RuntimeException e) {
                log.warn("failed to publish order cancel ingress stop signal", e);
                return null;
            }
            if (published) {
                return drained;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        log.warn("order cancel ingress ring is full during stop; skipping drain barrier");
        return null;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 50;
    }

    private static final class LoggingExceptionHandler
            implements ExceptionHandler<OrderCancelIngressEvent> {

        @Override
        public void handleEventException(Throwable ex, long sequence,
                                         OrderCancelIngressEvent event) {
            log.error("order cancel ingress handler failed at sequence {}", sequence, ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            log.error("order cancel ingress handler start failed", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            log.error("order cancel ingress handler shutdown failed", ex);
        }
    }
}
